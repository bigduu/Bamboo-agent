const assert = require("node:assert/strict")
const { createHash } = require("node:crypto")
const { existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } = require("node:fs")
const { spawnSync } = require("node:child_process")
const os = require("node:os")
const path = require("node:path")
const { test } = require("node:test")
const { runInNewContext } = require("node:vm")

const historical = require("./tests/main-authority-bootstrap-fixture.cjs").legacyData()
const legacyTest = (name, fn) => test(`historical: ${name}`, t => fn(t, historical.ci, id => job(id, historical.ci), historical.contributingRelease))

const workflow = readFileSync(".github/workflows/ci.yml", "utf8")
const cacheCleanupWorkflow = readFileSync(
  ".github/workflows/pr-cache-cleanup.yml",
  "utf8",
)
const codeqlWorkflow = readFileSync(".github/workflows/codeql.yml", "utf8")
const contributing = readFileSync("CONTRIBUTING.md", "utf8")
const frozenCacheWriteHeader = [
  "  BAMBOO_RELEASE_CACHE_WRITE_ALLOWED: >-",
  "    ${{ !(",
  "      startsWith(github.ref, 'refs/heads/bamboo/release/da348-') ||",
  "      github.ref == 'refs/heads/release/da348a63f1d019a22df547c78575c500b6b1294e' ||",
  "      startsWith(github.head_ref, 'bamboo/release/da348-') ||",
  "      github.head_ref == 'release/da348a63f1d019a22df547c78575c500b6b1294e' ||",
  "      (github.event_name == 'push' && github.ref == 'refs/heads/main' &&",
  "       (github.event.before == 'dbafb74522794012fc8f0a61e761817b3913989a' ||",
  "        contains(github.event.head_commit.message, 'release/da348a63f1d019a22df547c78575c500b6b1294e')))",
  "    ) }}",
]
const cacheSaveIf = (predicate) =>
  `save-if: \${{ (${predicate}) && env.BAMBOO_RELEASE_CACHE_WRITE_ALLOWED == 'true' }}`

const job = (id, source = workflow) => {
  const marker = `\n  ${id}:\n`
  const start = source.indexOf(marker)
  assert.notEqual(start, -1, `missing CI job: ${id}`)

  const contentStart = start + marker.length
  const nextJob = source.slice(contentStart).search(/\n  [a-z][a-z0-9-]*:\n/u)
  return nextJob === -1
    ? source.slice(contentStart)
    : source.slice(contentStart, contentStart + nextJob)
}

const comprehensiveOnly =
  "if: needs.validation-source.outputs.full == 'true'"

function assertCargoFailureCollection(source) {
  const commands = [...source.matchAll(/(?:run: )?cargo test --locked([^\n]*)/gu)]
    .map((match) => match[1].trim().split(/\s+/u))
    .filter((args) => !args.includes("--no-run"))
  assert.ok(commands.length > 0, "runtime Cargo commands are present")
  for (const args of commands) {
    const separator = args.indexOf("--")
    const cargoArgs = separator < 0 ? args : args.slice(0, separator)
    assert.ok(cargoArgs.includes("--no-fail-fast"), "collect failures across test executables")
  }
  assert.doesNotMatch(source, /continue-on-error:\s*true|\|\|\s*(?:true|exit 0)/u)
}

legacyTest("runtime Cargo gates collect failures without masking their nonzero exit", (_, workflow, job, contributing) => {
  const frontend = readFileSync(".github/workflows/frontend-acceptance.yml", "utf8")
  const macHelper = readFileSync("scripts/run-macos-server-lib-tests.sh", "utf8")
  for (const source of [workflow, frontend, macHelper]) {
    assertCargoFailureCollection(source)
    for (const match of source.matchAll(/cargo test --locked[^\n]*/gu)) {
      if (match[0].includes("--no-run")) continue
      const mutation = source.slice(0, match.index) +
        source.slice(match.index).replace(" --no-fail-fast", "")
      assert.throws(() => assertCargoFailureCollection(mutation))
    }
    for (const suffix of [" || true", " || exit 0"]) {
      assert.throws(() => assertCargoFailureCollection(`${source}\ntrue${suffix}\n`))
    }
  }
  // The macOS wrapper still propagates Cargo's recorded nonzero exit; the
  // existing executable wrapper fixture also verifies its exit-23 path.
  assert.match(macHelper, /cargo_status=\$\{pipeline_status\[0\]\}/u)
  assert.match(macHelper, /^exit "\$cargo_status"$/mu)
})

legacyTest("platform matrices retain results after another platform fails", (_, workflow, job, contributing) => {
  for (const id of ["tls-fixture-test", "tool-event-recorder-e2e", "build"]) {
    const source = job(id)
    assert.match(source, /^    strategy:\n      fail-fast: false\n/mu)
    assert.doesNotMatch(source, /continue-on-error:/u)
  }
})

test("standard release disables LTO without reducing optimization or accepting profile overrides", () => {
  const manifest = readFileSync("Cargo.toml", "utf8")
  const profile = manifest.match(/^\[profile\.release\]\n([\s\S]*?)(?=^\[|$(?![\s\S]))/mu)?.[1]
  assert.ok(profile)
  assert.match(profile, /^opt-level = 3$/mu)
  assert.match(profile, /^lto = "off"$/mu)
  const config = readFileSync(".cargo/config.toml", "utf8")
  assert.doesNotMatch(config, /^\[profile\.release(?:\.|\])|CARGO_PROFILE_RELEASE_|(?:^|\s)lto\s*=/mu)
  for (const source of [workflow, readFileSync(".github/workflows/publish-crate.yml", "utf8")]) {
    assert.doesNotMatch(source, /CARGO_PROFILE_RELEASE_|CARGO_ENCODED_RUSTFLAGS|(?:^|\s)RUSTFLAGS:/mu)
  }
  const builder = readFileSync("scripts/sidecar-build.cjs", "utf8")
  assert.ok(builder.includes("const args = ['build', '--locked', '--release', '--verbose', '--bin', 'bamboo', '--target', target,"))
  const { buildEnvironment } = require("./sidecar-build.cjs")
  for (const value of ["true", "false", "thin", "fat", "off"]) {
    assert.throws(() => buildEnvironment({ CARGO_PROFILE_RELEASE_LTO: value }, "/own/target"))
  }
  for (const overrides of [{ RUSTFLAGS: "-C lto=fat" }, { CARGO_ENCODED_RUSTFLAGS: "-Clto=thin" }]) {
    assert.throws(() => buildEnvironment(overrides, "/own/target"))
  }
})

test("macOS link guard preserves Cargo failures and rejects lost or oversized diagnostics", {
  skip: process.platform === "win32" && "the macOS helper requires a POSIX shell",
}, () => {
  const directory = mkdtempSync(path.join(os.tmpdir(), "bamboo-macos-link-guard-"))
  const helper = path.resolve(__dirname, "run-macos-server-lib-tests.sh")
  const expectedArgs = [
    "test", "--locked", "--no-fail-fast", "--profile", "dev", "-p", "bamboo-server",
    "--all-features", "--lib", "server::tls::tests", "--", "--nocapture",
  ]
  const stub = (name, body) => writeFileSync(path.join(directory, name), `#!/bin/bash\n${body}\n`, { mode: 0o755 })
  stub("uname", 'printf "%s\\n" Darwin')
  stub("cargo", [
    'printf "%s\\n" "$@" > "$FIXTURE_ARGS"',
    'printf "%s\\n" "test result: ok. 8 passed; 0 failed"',
    'if [[ "$FIXTURE_WARNING" == 1 ]]; then',
    '  echo "ld: __eh_frame section too large (max 16MB) to encode dwarf unwind offsets in compact unwind table" >&2',
    "fi",
    'exit "$FIXTURE_CARGO_STATUS"',
  ].join("\n"))
  const run = (overrides = {}) => spawnSync("bash", [helper], {
    encoding: "utf8",
    env: {
      PATH: `${directory}${path.delimiter}${process.env.PATH}`,
      TMPDIR: directory,
      FIXTURE_ARGS: path.join(directory, "cargo-args"),
      FIXTURE_WARNING: "0",
      FIXTURE_CARGO_STATUS: "0",
      ...overrides,
    },
  })
  try {
    const success = run()
    assert.equal(success.status, 0, success.stderr)
    assert.deepEqual(readFileSync(path.join(directory, "cargo-args"), "utf8").trim().split("\n"), expectedArgs)
    assert.match(success.stdout, /8 passed; 0 failed/u)

    const warning = run({ FIXTURE_WARNING: "1" })
    assert.equal(warning.status, 1)
    assert.match(warning.stderr, /emitted the oversized __eh_frame warning/u)

    const cargoFailure = run({ FIXTURE_CARGO_STATUS: "23" })
    assert.equal(cargoFailure.status, 23, cargoFailure.stderr)

    stub("tee", "cat >/dev/null\nexit 13")
    const captureFailure = run()
    assert.equal(captureFailure.status, 1)
    assert.match(captureFailure.stderr, /failed to capture.*linker diagnostics/u)
    rmSync(path.join(directory, "tee"))

    stub("grep", "exit 2")
    const inspectFailure = run()
    assert.equal(inspectFailure.status, 1)
    assert.match(inspectFailure.stderr, /failed to inspect.*linker diagnostics/u)
  } finally {
    rmSync(directory, { recursive: true, force: true })
  }
})

legacyTest("main-push validation remains reachable by the existing automatic publisher", (_, workflow, job, contributing) => {
  assert.match(workflow, /^  push:\n    branches: \[ main \]$/mu)
  assert.match(workflow, /pull_request:\n\s+branches: \[ dev, main \]/u)

  const testJob = job("test")
  assert.match(testJob, /name: Test\n/u)
  assert.match(testJob, /runs-on: ubuntu-latest\n/u)
  assert.ok(testJob.includes("    timeout-minutes: ${{ github.event_name == 'pull_request' && github.base_ref == 'dev' && 45 || 75 }}"))
  assert.doesNotMatch(testJob, /cargo build/u)
  assert.match(testJob, /test "\$FEATURE_RESULT" = success/u)
  assert.match(testJob, /test "\$CRATE_RESULT" = success/u)
  assert.match(testJob, /test "\$SSH_RESULT" = success/u)
  assert.doesNotMatch(testJob, /run: cargo test/u)
  assert.match(
    testJob,
    /- name: Test CI workflow policy\n        if: needs\.validation-source\.outputs\.full == 'true'\n        run: node --test scripts\/ci-policy\.test\.cjs scripts\/crate-release\.test\.cjs scripts\/ci-routing\.test\.cjs scripts\/sidecar-release\.test\.cjs scripts\/test-groups\.test\.cjs scripts\/run-feature-tests\.test\.cjs scripts\/native-test-owner\.test\.cjs\n/u,
  )
  assert.match(
    testJob,
    /- name: Check formatting\n        if: needs\.validation-source\.outputs\.full == 'true'\n        run: cargo fmt --all -- --check\n/u,
  )
  assert.ok(testJob.includes("node-version: '24'"))
  assert.ok(testJob.indexOf("- name: Setup Node.js") < testJob.indexOf("- name: Test CI workflow policy"))

  for (const name of [
    "Test frontend artifact and release policies",
    "Stage frontend package",
    "Verify published server crate owns the frontend package",
  ]) {
    assert.ok(
      testJob.includes(`name: ${name}\n        ${comprehensiveOnly}`),
      `${name} must be comprehensive-only`,
    )
  }

  assert.doesNotMatch(testJob, /run: cargo test --locked --no-fail-fast --all-features/u)
  assert.match(job("test-crates"), /run: cargo test --locked --no-fail-fast -p "\$TEST_CRATE"\n/u)
  assert.match(job("test-ssh"), /run: bash scripts\/run-russh-live\.sh/u)

  assert.match(
    contributing,
    /feature PR into `dev` runs required `Test`/u,
  )
  assert.match(contributing, /Before merge, owners must run affected tests locally and add or update coverage/u)
  assert.match(contributing, /for changed behavior, recording exact source, commands and results/u)
})

function crateMatrix(source) {
  const list = source.match(/      matrix:\n        crate:\n((?:          - [a-z0-9-]+\n)+)/u)
  assert.ok(list, "native crate matrix is present")
  return [...list[1].matchAll(/          - ([a-z0-9-]+)/gu)].map((match) => match[1])
}

const serverBlocks = ["browser", "chat", "execute", "settings", "app-state", "handlers", "other", "integration"]
const serverFilters = ["browser::", "handlers::agent::chat::", "handlers::agent::execute::", "handlers::settings::", "app_state::", "handlers::"]

function serverMatrix(source) {
  return [...source.matchAll(/^          - crate: ([a-z0-9-]+)\n            server_block: ([a-z-]+)$/gmu)]
    .map((match) => ({ crate: match[1], block: match[2] }))
}

legacyTest("legacy release matrices retain their original crate and server coverage", (_, workflow, job, contributing) => {
  const manifest = readFileSync("Cargo.toml", "utf8")
  const members = manifest.match(/^default-members = \[([\s\S]*?)^\]/mu)
  assert.ok(members)
  const packages = [...members[1].matchAll(/^\s*"([^"]+)"/gmu)].map((match) => {
    const content = readFileSync(path.join(match[1], "Cargo.toml"), "utf8")
    return content.match(/^\[package\]\nname = "([^"]+)"/mu)[1]
  }).sort()
  assert.ok(packages.length > 0)
  for (const id of ["test-crates", "e2e-crates"]) {
    const source = job(id)
    assert.deepEqual(crateMatrix(source).sort(), packages.filter((name) => name !== "bamboo-server"))
    assert.deepEqual(serverMatrix(source), serverBlocks.map((block) => ({ crate: "bamboo-server", block })))
    assert.ok(source.includes("${{ matrix.server_block && format('/{0}', matrix.server_block) || '' }}"))
    assert.match(source, /fail-fast: false/u)
    assert.match(source, /timeout-minutes: 75/u)
    assert.ok(source.includes('TEST_CRATE: ${{ matrix.crate }}'))
    assert.ok(source.includes('SERVER_TEST_BLOCK: ${{ matrix.server_block }}'))
    assert.match(source, /if: matrix\.crate != 'bamboo-server'\n        run: cargo test/u)
    assert.match(source, /if: matrix\.crate == 'bamboo-server'\n        shell: bash/u)
    assert.ok(source.includes("${{ matrix.server_block && format('-{0}', matrix.server_block) || '' }}"))
    assert.ok(source.includes(comprehensiveOnly))
    assert.doesNotMatch(source, /max-parallel:|continue-on-error:|command: (build|run)|npm ci|--test-threads=1/u)
  }
  assert.match(job("test-crates"), /run: cargo test --locked --no-fail-fast -p "\$TEST_CRATE"\n/u)
  assert.match(job("e2e-crates"), /run: cargo test --locked --no-fail-fast -p "\$TEST_CRATE" --all-features --lib --tests\n/u)
  assert.doesNotMatch(workflow, /test-build:|e2e-build:|test-blocks:|test-doctests:|grouped-tests|ci-artifacts|plan-id:/u)
  assert.match(workflow, /^permissions:\n  contents: read\n/mu)
  assert.match(job("main-release-admission"), /^    permissions:\n      contents: read\n      actions: read\n/mu)
})

test("one feature executor preserves default configurations and complete explicit additive case ownership", () => {
  const { selectedPackages, sourceFeatureHash, FEATURE_SOURCE_SHA256, FEATURE_CONTEXTS, cargoArguments } = require('./run-feature-tests.cjs')
  const result = spawnSync('cargo', ['metadata', '--locked', '--format-version', '1', '--no-deps'], { encoding: 'utf8', maxBuffer: 16 * 1024 * 1024 })
  assert.equal(result.status, 0, result.stderr)
  const metadata = JSON.parse(result.stdout), packages = selectedPackages(metadata)
  const expected = metadata.packages.filter(item => metadata.workspace_default_members.includes(item.id) || item.name === 'bamboo-tool-event-recorder-example')
  assert.deepEqual(packages.map(item => item.name).sort(), expected.map(item => item.name).sort())
  assert.ok(!packages.some(item => item.name === 'bamboo-analytics'))
  const kinds = packages.flatMap(item => item.targets.filter(target => target.test).map(target => ({ id: `${item.id}/${target.name}/${target.kind}`, kind: target.kind })))
  const unit = kinds.filter(target => target.kind.some(kind => ['lib', 'bin'].includes(kind))).map(target => target.id)
  const e2e = kinds.filter(target => target.kind.includes('test')).map(target => target.id)
  assert.ok(unit.length > 0 && e2e.length > 0)
  assert.deepEqual(unit.filter(id => e2e.includes(id)), [])
  for (const owner of packages) {
    const args = cargoArguments(owner)
    assert.equal(args.filter(arg => arg === '-p').length, 1)
    assert.equal(args[args.indexOf('-p') + 1], owner.name)
    assert.ok(!args.some(arg => ['--all-features','--features','--lib','--bins','--test','--tests','--doc','--all-targets'].includes(arg)))
  }
  assert.equal(sourceFeatureHash(packages, require('node:fs').realpathSync('.')), FEATURE_SOURCE_SHA256,
    'New feature-sensitive source requires a reviewed ownership update')
  assert.equal(FEATURE_CONTEXTS.flatMap(context => context.targets.flatMap(target => target.added)).length, 19)
  assert.deepEqual(FEATURE_CONTEXTS.map(context => [context.package, context.features]), [
    ['bamboo-infrastructure', ['otel-otlp', 'test-utils']], ['bamboo-tickets', ['test-utils']],
    ['bamboo-server', ['test-utils']], ['bamboo-agent', ['ticket-runtime-fixtures']]])
  for (const context of FEATURE_CONTEXTS) {
    assert.ok(packages.some(owner => owner.name === context.package))
    assert.ok(!cargoArguments({ name: context.package }, context, true).includes('--all-features'))
  }
  assert.match(job('feature-tests'), /target\/feature-tests/u)
  assert.match(job('feature-tests'), /run: node scripts\/run-feature-tests.cjs/u)
})

test("the feature executor bounds debug artifacts and records capacity without changing release profiles", () => {
  const feature = job("feature-tests")
  assert.match(feature, /^    timeout-minutes: 90$/mu)
  for (const profile of ["DEV", "TEST"]) {
    const setting = `CARGO_PROFILE_${profile}_DEBUG: "0"`
    assert.ok(feature.includes(setting))
    assert.equal(workflow.split(setting).length - 1, 1, "debug override belongs only to the feature job")
  }
  assert.match(workflow, /^  CARGO_INCREMENTAL: "0"$/mu)
  assert.match(feature, /rustc -vV\n            cargo -V/u)
  assert.match(feature, /df -Pk "\$GITHUB_WORKSPACE" "\$RUNNER_TEMP"/u)
  assert.match(feature, /- name: Record final feature disk usage\n        if: always\(\)/u)
  assert.match(feature, /du -sk "\$CARGO_TARGET_DIR"/u)
  assert.match(feature, /\$\{\{ runner.temp \}\}\/bamboo-feature-profile.log/u)
  assert.doesNotMatch(feature, /cargo (?:build|check|clean|test)|--release|CARGO_PROFILE_RELEASE_|rm -[a-z]*r/u)
  assert.doesNotMatch(readFileSync(".github/workflows/publish-crate.yml", "utf8"), /CARGO_PROFILE_(?:DEV|TEST)_DEBUG/u)
})

legacyTest("required Test performs only result admission for feature PRs without an unused checkout, toolchain or cache", (_, workflow, job, contributing) => {
  const assertOwnership = (source) => {
    const steps = source.split(/\n      - /u).slice(1)
    assert.ok(steps.length > 1)
    assert.match(steps[0], /^name: Require validation admission\n/u)
    for (const step of steps.slice(1)) {
      assert.match(step, /^        if: (?:(?:always|failure)\(\) && )?needs\.validation-source\.outputs\.full == 'true'$/mu)
    }
  }
  const aggregate = job("test")
  assertOwnership(aggregate)
  for (const name of ["Install Rust", "Record the full validation toolchain", "Rust cache"]) {
    assert.throws(() => assertOwnership(aggregate.replace(`name: ${name}\n        if: needs.validation-source.outputs.full == 'true'`,
      `name: ${name}\n        if: github.event_name != 'push'`)))
  }
})

test("serial Root birth selectors reuse canonical storage compilation and preserve strict lint ownership", () => {
  const { SERIAL_STORAGE_FAMILIES, SERIAL_STORAGE_FILTERS, SERIAL_STORAGE_SELECTORS, ROOT_BIRTH_CASES, cargoArguments } = require("./run-feature-tests.cjs")
  assert.deepEqual(SERIAL_STORAGE_FAMILIES, ["v2::root_birth_tests::", "v2::host_root_create::tests::",
    "v2::supervisor_management_tests::", "v2::supervisor_management_lifetime_tests::", "v2::supervisor_proof_tests::",
    "v2::supervisor_management::observer_lock_order_tests::"])
  assert.deepEqual(SERIAL_STORAGE_FILTERS, ["root_birth_tests", "host_root_create::tests", "supervisor_management", "supervisor_proof"])
  const source = readFileSync("crates/infra/bamboo-storage/src/v2/root_birth_tests.rs", "utf8")
  assert.deepEqual([...source.matchAll(/async fn (root_birth_[a-z_]+)\(/gu)].map(match => match[1]).sort(), [...ROOT_BIRTH_CASES].sort())
  const args = cargoArguments({ name: "bamboo-storage" })
  assert.deepEqual(args.slice(args.indexOf("--") + 1), SERIAL_STORAGE_SELECTORS.flatMap(prefix => ["--skip", prefix]))
  const lint = readFileSync(".github/workflows/root-birth-acceptance.yml", "utf8")
  assert.doesNotMatch(lint, /cargo test/u)
  assert.match(lint, /cargo clippy --locked --all-features --lib -p bamboo-storage -- -D warnings/u)
  assert.match(lint, /test "\$actual" = "\$EXPECTED_SOURCE"/u)
})

const OBSERVER_FAULT_HOST_CASES = [
  "fault_cap_from_actual_publication_failure_notifies_idle_parent_once",
  "fault_preserves_live_work_wait_and_original_user_input",
  "fault_parent_inbox_before_checkpoint_reopens_and_never_replays_provider",
  "fault_missing_source_can_report_but_revoked_registration_cannot",
  "fault_delivery_failure_is_bounded_durable_and_preserves_original_payload",
]

function assertSubscriptionException(source) {
  // Keep the complete original joint recipe as an explicit manual diagnostic.
  // Only its entrypoint/source binding and the separate Clippy owner changed.
  // This remains API-only feature-head evidence, never canonical-M admission.
  const extension = "          faults = {\n" + OBSERVER_FAULT_HOST_CASES
    .map(name => `              '${name}',\n`).join("") +
    "          }\n          expected |= {f'app_state::observer_subscriptions::tests::fault::{name}' for name in faults}\n"
  assert.ok(source.includes(extension), "Require all five exact fault cases")
  const original = source
    .replace("      - 'crates/app/bamboo-server/src/app_state/observer_fault_tests.rs'\n", "")
    .replace(extension, "")
    .replace("print('Six canonical windows, exact legacy serde, five fail-closed Inbox regressions, seven subscription and five fault Host fixtures passed')",
      "print('Six canonical windows, exact legacy serde, five fail-closed Inbox regressions and all seven actual subscription Host fixtures passed')")
  assert.equal(createHash("sha256").update(original).digest("hex"), "5a9ec1ee32d1e0faaee714a21bba5e64775b6a2bc61bc0ea90c88f29f6823431")
  assert.equal(createHash("sha256").update(source).digest("hex"), "4ba96e60f4fb4b2a28aa1614e385a76f90f1c9d648d4a17a5a78166e72b4a0e5")
  assert.match(source, /on:\n  workflow_dispatch:\n/u)
  assert.doesNotMatch(source.split("\npermissions:")[0], /pull_request:|push:/u)
  assert.match(source, /runs-on: ubuntu-latest/u)
  assert.match(source, /BAMBOO_FRONTEND_BUILD_MODE: api-only/u)
  assert.match(source, /EXPECTED_SOURCE_SHA: \$\{\{ github\.sha \}\}/u)
  assert.match(source, /ref: \$\{\{ github\.sha \}\}/u)
  assert.match(source, /test "\$actual" = "\$EXPECTED_SOURCE_SHA"/u)
  assert.match(source, /cargo test --locked -p bamboo-domain -p bamboo-engine -p bamboo-server --lib --/u)
  assert.match(source, /--test-threads=1 --color=never/u)
  assert.doesNotMatch(source, /cargo clippy/u)
}

const OBSERVER_SHARED_SERDE_CASE = "session::supervisor_management::tests::supervisor_management_public_serde_is_legacy_compatible_and_children_drop_state"
function observerRecipe(source, name) {
  const marker = `      - name: ${name}\n`, start = source.indexOf(marker)
  assert.notEqual(start, -1, `Missing Observer recipe ${name}`)
  const end = source.indexOf("\n      - ", start + marker.length)
  return source.slice(start, end === -1 ? undefined : end).trimEnd()
}
function assertObserverRuntimeOwner(definitions) {
  const combined = definitions["trusted-observer.yml"], manual = definitions["observer-subscriptions.yml"]
  assertLinuxTestOwnership({ "trusted-observer.yml": combined, "observer-subscriptions.yml": manual })
  const paths = source => [...source.split("\npermissions:")[0].matchAll(/^      - '([^']+)'$/gmu)].map(row => row[1]).sort()
  assert.deepEqual(paths(combined), paths(definitions["observer-api-lint.yml"]).filter(file => file !== ".github/workflows/observer-api-lint.yml"))
  assert.match(combined, /pull_request:\n    branches: \[dev\]/u)
  assert.doesNotMatch(manual.split("\npermissions:")[0], /pull_request:|push:/u)
  assert.match(combined, /timeout-minutes: 180/u)
  const firstName = "Run real management, proof, lock-order and native Host cases"
  const secondName = "Run exact canonical state and seven real subscription Host cases"
  assert.ok(combined.indexOf(firstName) < combined.indexOf(secondName))
  const first = observerRecipe(combined, firstName), second = observerRecipe(combined, secondName)
  assert.match(first, /timeout-minutes: 90/u)
  assert.ok(first.includes(`assert '${OBSERVER_SHARED_SERDE_CASE}' in results['domain']`), "The first joint recipe must prove the shared case passed")
  const expected = observerRecipe(manual, secondName)
    .replace(`name: ${secondName}\n`, `name: ${secondName}\n        if: success()\n        timeout-minutes: 90\n`)
    .replace('            --test-threads=1 --color=never \\\n', `            --skip ${OBSERVER_SHARED_SERDE_CASE} \\\n            --test-threads=1 --color=never \\\n`)
    .replace(`assert results['domain'] - window == {'${OBSERVER_SHARED_SERDE_CASE}'}`, "assert results['domain'] == window, 'Shared serde case already passed in the first joint recipe'")
    .replace("Six canonical windows, exact legacy serde,", "Six canonical windows, shared serde already covered,")
  assert.equal(second, expected, "Preserve the complete original joint recipe, skipping only the already-passed serde case")
  assert.equal((combined.match(/--skip /gu) ?? []).length, 1)
  assert.match(second, /cargo test --locked -p bamboo-domain -p bamboo-engine -p bamboo-server --lib --/u)
  assert.equal((combined.match(/CARGO_TARGET_DIR:/gu) ?? []).length, 1)
}

test("one automatic Observer job preserves both joint graphs and executes shared serde once", () => {
  const definitions = Object.fromEntries(readdirSync(".github/workflows").filter(file => /\.ya?ml$/u.test(file))
    .map(file => [file, readFileSync(path.join(".github/workflows", file), "utf8")]))
  assertObserverRuntimeOwner(definitions)
  const name = "trusted-observer.yml", source = definitions[name]
  for (const [before, after] of [[`--skip ${OBSERVER_SHARED_SERDE_CASE}`, "--skip supervisor_management"],
    [`--skip ${OBSERVER_SHARED_SERDE_CASE}`, ""], ["        if: success()", "        if: always()"],
    ["-p bamboo-domain -p bamboo-engine -p bamboo-server --lib", "-p bamboo-engine -p bamboo-server --lib"],
    ["Shared serde case already passed in the first joint recipe", "ignored duplicate"],
    ["assert len(window) == 6", "assert len(window) >= 0"]]) {
    assert.ok(source.includes(before), before)
    assert.throws(() => assertObserverRuntimeOwner({ ...definitions, [name]: source.replace(before, after) }))
  }
  assert.throws(() => assertObserverRuntimeOwner({ ...definitions, "observer-subscriptions.yml": definitions["observer-subscriptions.yml"]
    .replace("  workflow_dispatch:", "  pull_request:\n    branches: [dev]") }))
})

function assertObserverLintOwner(definitions) {
  const filename = "observer-api-lint.yml", owner = definitions[filename]
  assert.equal(typeof owner, "string", "Require the single shared Observer lint owner")
  const paths = source => (source.split("\npermissions:\n")[0].match(/^      - .+$/gmu) ?? []).map(line => {
    assert.match(line, /^      - '[^']+'$/u)
    return line.slice(9, -1)
  })
  const originals = ["trusted-observer.yml", "observer-subscriptions.yml"]
  const expected = [...new Set([...originals.flatMap(file => paths(definitions[file])), `.github/workflows/${filename}`])].sort()
  assert.deepEqual(paths(owner).sort(), expected, "Preserve the complete union of both runtime path filters")
  const trigger = owner.split("\non:\n")[1].split("\npermissions:\n")[0]
  assert.deepEqual(trigger.match(/^  \S.*$/gmu), ["  pull_request:"])
  assert.match(trigger, /^    branches: \[dev\]$/mu)
  assert.deepEqual([...owner.split("\njobs:\n")[1].matchAll(/^  ([a-z-]+):$/gmu)].map(row => row[1]), ["lint"])
  assert.match(owner, /runs-on: ubuntu-latest/u)
  assert.match(owner, /uses: dtolnay\/rust-toolchain@stable\n        with:\n          components: clippy/u)
  for (const line of ["CARGO_BUILD_JOBS: '1'", "CARGO_INCREMENTAL: '0'", "RUST_MIN_STACK: '8388608'",
    "BAMBOO_FRONTEND_BUILD_MODE: api-only", "EXPECTED_SOURCE_SHA: ${{ github.event.pull_request.head.sha }}",
    "ref: ${{ github.event.pull_request.head.sha }}", "save-if: false"]) assert.ok(owner.includes(line), line)
  assert.equal((owner.match(/test "\$actual" = "\$EXPECTED_SOURCE_SHA"/gu) ?? []).length, 2)
  assert.equal((owner.match(/test -z "\$status"/gu) ?? []).length, 2)
  assert.match(owner, /name: Verify exact source stayed clean\n        if: always\(\)/u)
  assert.doesNotMatch(owner, /continue-on-error|CARGO_PROFILE_|RUSTFLAGS|cargo test|npm |secrets\./u)
  const normalized = owner.replace(/\\\r?\n[ \t]*/gu, " ").replace(/[ \t]+/gu, " ")
  const policy = normalized.match(/policy=\(([\s\S]*?)\)/u)?.[1].trim().split(/\s+/u)
  assert.deepEqual(policy, ["-D", "warnings", "-A", "deprecated", "-A", "clippy::module_inception", "-A", "clippy::incompatible_msrv",
    "-A", "clippy::needless_borrows_for_generic_args", "-A", "clippy::too-many-arguments", "-A", "clippy::while-let-loop",
    "-A", "clippy::type_complexity", "-A", "dead-code"])
  assert.match(normalized, /cargo clippy --locked --all-features --lib -p bamboo-domain -p bamboo-storage -p bamboo-engine -p bamboo-server-tools -p bamboo-server -- "\$\{policy\[@\]\}"/u)
  assert.doesNotMatch(normalized, /cargo clippy[^\n]*(?:--workspace|--tests|--bins)/u)
  const packages = ["bamboo-domain", "bamboo-engine", "bamboo-server", "bamboo-server-tools", "bamboo-storage"]
  const owners = Object.entries(definitions).flatMap(([file, source]) => {
    if (!source.includes("BAMBOO_FRONTEND_BUILD_MODE: api-only")) return []
    return [...source.replace(/\\\r?\n[ \t]*/gu, " ").matchAll(/cargo clippy[^\n]+/gu)].filter(([command]) =>
      JSON.stringify([...command.matchAll(/(?:^| )-p ([a-z-]+)/gu)].map(row => row[1]).sort()) === JSON.stringify(packages)).map(() => file)
  })
  assert.deepEqual(owners, [filename], "The identical five-library API-only analysis must execute once")
  for (const file of originals) assert.doesNotMatch(definitions[file], /cargo clippy/u)
}

test("Observer API-only Clippy has one owner for the union of both runtime paths", () => {
  const definitions = Object.fromEntries(readdirSync(".github/workflows").filter(file => /\.ya?ml$/u.test(file))
    .map(file => [file, readFileSync(path.join(".github/workflows", file), "utf8")]))
  assertObserverLintOwner(definitions)
  const name = "observer-api-lint.yml", owner = definitions[name]
  for (const [before, after] of [["BAMBOO_FRONTEND_BUILD_MODE: api-only", "BAMBOO_FRONTEND_BUILD_MODE: embedded"],
    ["ref: ${{ github.event.pull_request.head.sha }}", "ref: ${{ github.sha }}"],
    ["branches: [dev]", "branches: [main]"], ["--all-features", "--no-default-features"],
    ["-p bamboo-server-tools ", ""], ["-D warnings", "-A warnings"], ['test -z "$status"', "true"],
    ["save-if: false", "save-if: true"], ["  pull_request:", '  "push":\n  pull_request:']]) {
    assert.ok(owner.includes(before), before)
    assert.throws(() => assertObserverLintOwner({ ...definitions, [name]: owner.replace(before, after) }))
  }
  for (const file of ["crates/core/bamboo-domain/src/session/supervisor_management.rs",
    "crates/infra/bamboo-storage/src/v2/child_wait_control_plane.rs", "crates/app/bamboo-server/src/app_state/builder.rs"]) {
    assert.throws(() => assertObserverLintOwner({ ...definitions, [name]: owner.replace(`      - '${file}'\n`, "") }))
  }
  assert.throws(() => assertObserverLintOwner({ ...definitions, [name]: undefined }))
  assert.throws(() => assertObserverLintOwner({ ...definitions, "extra-api-lint.yml": owner }))
})

function assertLinuxTestOwnership(definitions) {
  const registered = {
    "ci.yml": {
      "feature-tests": "The canonical isolated-default and explicit-feature test executor",
      "quick-checks": "Small synthetic workflow/controller policy fixtures",
      test: "Pinned legacy release policy fixtures; feature mode is admission only",
      "test-crates": "Pinned legacy release Unit acceptance until source cutover",
      "e2e-crates": "Pinned legacy release E2E acceptance until source cutover",
      "test-ssh": "Pinned legacy release live SSH fixture until source cutover",
      build: "Pinned legacy release native-platform tests until source cutover",
      "tool-event-recorder-e2e": "Pinned legacy recorder coverage; feature runs in its owned native matrix",
    },
    "frontend-acceptance.yml": {
      build: "Pinned legacy original-release-head build and policy fixtures",
      test: "Pinned legacy original-release-head consumer contract",
    },
    "release-cargo-isolation.yml": { isolation: "Synthetic credential/ENOSPC containment boundary" },
    "trusted-observer.yml": { observer: "Accepted API-only/F multi-package Observer configuration; not M evidence" },
    "delegated-planner-acceptance.yml": { planner: "Distinct API-only/F Planner CLI and original Clippy; library cases run once on canonical M" },
    "observer-subscriptions.yml": { observer: "Inherited 6cfe API-only/F subscription joint graph; temporary exact configuration, not M evidence" },
    "ticket-platform-adapter.yml": { "native-budget": "Explicit manual snapshot diagnosis; normal PR job is Windows" },
  }
  for (const [file, source] of Object.entries(definitions)) {
    if (file === "observer-subscriptions.yml") assertSubscriptionException(source)
    if (!/^  pull_request:\s*$/mu.test(source)) continue
    const jobs = source.slice(source.indexOf("\njobs:\n") + 7)
      .split(/(?=^  [a-z][a-z0-9-]*:\n)/mu).filter(Boolean)
    for (const block of jobs) {
      if (!/ubuntu|linux\//iu.test(block)) continue
      const id = block.match(/^  ([a-z][a-z0-9-]*):/u)?.[1]
      const scripts = [...block.matchAll(/(?:node(?: --test)?|bash|python3?) (scripts\/[a-zA-Z0-9_./-]+\.(?:cjs|mjs|js|py|sh))/gu)]
        .map(match => match[1]).filter(name => existsSync(name)).map(name => readFileSync(name, "utf8"))
      const code = [block, ...scripts].join("\n")
      if (!/\bcargo(?: \+[^\s]+)? test\b/u.test(code) &&
          !(/["']cargo["']/u.test(code) && /["']test["']/u.test(code))) continue
      assert.ok(registered[file]?.[id], `Ordinary Linux Rust tests must join the canonical feature executor: ${file}:${id}; consolidate targets or review an explicit scoped exception`)
      if (file === "delegated-planner-acceptance.yml") {
        // Retain #2304 API-only CLI/lint; default library runtime has moved to M.
        assert.equal(createHash("sha256").update(source).digest("hex"), "e40d291b67054a05629a85a8b802fe198a0ee6e3b0d9beb8102e09bd744509ff")
        assert.match(block, /BAMBOO_FRONTEND_BUILD_MODE: api-only/u)
        assert.match(block, /EXPECTED_SOURCE_SHA: \$\{\{ github\.event\.pull_request\.head\.sha \}\}/u)
        assert.match(block, /ref: \$\{\{ github\.event\.pull_request\.head\.sha \}\}/u)
        assert.match(block, /test "\$\(git rev-parse HEAD\)" = "\$EXPECTED_SOURCE_SHA"/u)
        assert.doesNotMatch(block, /cargo test --locked -p bamboo-(?:server-tools|engine) --lib/u)
        assert.ok(block.includes("-p bamboo-engine -p bamboo-server-tools -p bamboo-server --lib --"))
        for (const command of ["cargo test --locked -p bamboo-agent --test required_child_context_packet",
          "cargo clippy --locked --all-features -p bamboo-agent"]) assert.ok(block.includes(command))
      }
      if (file === "trusted-observer.yml") {
        // Preserve this separately accepted F/config exactly; never relabel it as M.
        assert.equal(createHash("sha256").update(source).digest("hex"), "ce58547220ee0620820911b4354af9d446acd65bb036cbfb1ce5411360acebaa")
        assert.match(block, /BAMBOO_FRONTEND_BUILD_MODE: api-only/u)
        assert.match(block, /EXPECTED_SOURCE_SHA: \$\{\{ github\.event\.pull_request\.head\.sha \}\}/u)
        assert.match(block, /ref: \$\{\{ github\.event\.pull_request\.head\.sha \}\}/u)
        assert.match(block, /test "\$actual" = "\$EXPECTED_SOURCE_SHA"/u)
        assert.match(block, /cargo test --locked -p bamboo-domain -p bamboo-storage -p bamboo-server --lib --/u)
        assert.doesNotMatch(block, /cargo clippy/u)
      }
      if (file === "ticket-platform-adapter.yml") {
        assert.match(block, /^    if: github.event_name == 'workflow_dispatch' && inputs.snapshot_sha != ''$/mu)
      }
      if (file === "frontend-acceptance.yml") assert.match(source, /pull_request:\n    branches: \[main\]/u)
      if (file === "ci.yml" && ["test-crates", "e2e-crates", "test-ssh"].includes(id)) {
        assert.match(block, /^    if: needs.validation-source.outputs.full == 'true'$/mu)
      }
      if (file === "ci.yml" && id === "build") {
        assert.match(block, /^    if: >-\n      needs.validation-source.outputs.full == 'true' &&\n/mu)
        assert.doesNotMatch(block, /outputs\.feature|base_ref == 'dev'/u)
      }
      if (file === "ci.yml" && id === "feature-tests") {
        assert.equal([...block.matchAll(/run: node scripts\/run-feature-tests\.cjs$/gmu)].length, 1)
        assert.doesNotMatch(block, /cargo test/u)
        const paths = [...block.matchAll(/\bscripts\/[a-zA-Z0-9_./-]+\.(?:cjs|mjs|js|py|sh)/gu)].map(match => match[0])
        assert.ok(paths.every(name => ["scripts/run-feature-tests.cjs", "scripts/frontend-package.cjs"].includes(name)))
      }
      if (file === "ci.yml" && id === "tool-event-recorder-e2e") {
        assert.match(block, /^    if: needs.validation-source.outputs.full == 'true'$/mu)
      }
    }
  }
}

test("all agents share canonical feature tests instead of adding ordinary Linux PR producers", () => {
  const directory = ".github/workflows"
  const definitions = Object.fromEntries(readdirSync(directory).filter(file => /\.ya?ml$/u.test(file))
    .map(file => [file, readFileSync(path.join(directory, file), "utf8")]))
  assertLinuxTestOwnership(definitions)
  assertObserverLintOwner(definitions)
  assertObserverRuntimeOwner(definitions)
  for (const [before, after] of [["BAMBOO_FRONTEND_BUILD_MODE: api-only", "BAMBOO_FRONTEND_BUILD_MODE: embedded"],
    ["ref: ${{ github.event.pull_request.head.sha }}", "ref: ${{ github.sha }}"],
    ["-p bamboo-domain -p bamboo-storage -p bamboo-server --lib", "-p bamboo-server --lib"]]) {
    assert.throws(() => assertLinuxTestOwnership({ ...definitions,
      "trusted-observer.yml": definitions["trusted-observer.yml"].replace(before, after) }))
  }
  for (const [before, after] of [["BAMBOO_FRONTEND_BUILD_MODE: api-only", "BAMBOO_FRONTEND_BUILD_MODE: embedded"],
    ["ref: ${{ github.event.pull_request.head.sha }}", "ref: ${{ github.sha }}"],
    ["--test required_child_context_packet", "--lib"], ["--all-features", "--no-default-features"]]) {
    assert.throws(() => assertLinuxTestOwnership({ ...definitions,
      "delegated-planner-acceptance.yml": definitions["delegated-planner-acceptance.yml"].replace(before, after) }))
  }
  const duplicate = "name: New delegated planner acceptance\non:\n  pull_request:\n    branches: [dev]\njobs:\n  planner:\n    runs-on: ubuntu-latest\n    steps:\n      - run: cargo test --locked -p bamboo-agent --test required_child_context_packet\n"
  assert.throws(() => assertLinuxTestOwnership({ ...definitions, "unregistered-planner-acceptance.yml": duplicate }), /must join the canonical feature executor/u)
  const unregistered = workflow + "\n  extra-linux-tests:\n    runs-on: ubuntu-latest\n    steps:\n      - run: cargo test --locked -p bamboo-server --lib\n"
  assert.throws(() => assertLinuxTestOwnership({ ...definitions, "ci.yml": unregistered }), /must join the canonical feature executor/u)
  assert.throws(() => assertLinuxTestOwnership({ ...definitions, "ticket-platform-adapter.yml": definitions["ticket-platform-adapter.yml"]
    .replace("  workflow_dispatch:\n", "  pull_request:\n    branches: [dev]\n  workflow_dispatch:\n")
    .replace("if: github.event_name == 'workflow_dispatch' && inputs.snapshot_sha != ''", "if: github.event_name == 'pull_request'") }), /workflow_dispatch/u)
})

test("Supervisor API-only diagnostics are manual and bind the exact dispatch before checkout", () => {
  const source = readFileSync(".github/workflows/supervisor-role-acceptance.yml", "utf8")
  assert.match(source, /^  workflow_dispatch:$/mu)
  assert.doesNotMatch(source, /^  (?:pull_request|push):/mu)
  assert.match(source, /snapshot_sha:\n        description: .+\n        required: true\n        type: string/u)
  assert.match(source, /EXPECTED_SOURCE_SHA: \$\{\{ inputs\.snapshot_sha \}\}/u)
  assert.match(source, /DISPATCH_SOURCE_SHA: \$\{\{ github\.sha \}\}/u)
  assert.match(source, /BAMBOO_FRONTEND_BUILD_MODE: api-only/u)
  assert.match(source, /cancel-in-progress: false/u)
  assert.doesNotMatch(source, /continue-on-error: true|\|\| true/u)
  const marker = "      - name: Validate exact diagnostic source\n        run: |\n"
  assert.ok(source.indexOf(marker) < source.indexOf("      - uses: actions/checkout@"))
  const script = source.split(marker)[1].split("\n      - ")[0].split("\n")
    .map(line => line.replace(/^          /u, "")).join("\n")
  const sha = "a".repeat(40)
  for (const [snapshot, dispatched, valid] of [[sha, sha, true], [sha, "b".repeat(40), false],
    ["A".repeat(40), "A".repeat(40), false], ["a".repeat(39), "a".repeat(39), false],
    ["", "", false], [sha + "\n", sha, false], ["$(exit 0)", sha, false]]) {
    const result = spawnSync("bash", ["-c", script], { encoding: "utf8",
      env: { PATH: process.env.PATH, EXPECTED_SOURCE_SHA: snapshot, DISPATCH_SOURCE_SHA: dispatched } })
    assert.equal(result.status === 0, valid, "foreign/malformed diagnostic source must fail closed")
  }
  const ownerCase = "only_canonical_ticket_child_of_the_current_supervisor_avoids_orphan_wait"
  const ownerBlock = source.split("run_nonempty work-owner")[1].split("run_nonempty engine")[0]
  assert.ok(ownerBlock.includes(`ticket_runtime::tests::${ownerCase}`))
  assert.ok(ownerBlock.includes("-- --exact --test-threads=1"))
  assert.ok(source.includes(`--skip ${ownerCase}`), "the explicit case cannot run twice in the broad selector")
  assert.equal((source.match(/test "\$\(git rev-parse HEAD\)" = "\$EXPECTED_SOURCE_SHA"/gu) || []).length, 2)
})

test("Original Epic API-only diagnostic binds its exact dispatch source", () => {
  const source = readFileSync(".github/workflows/supervisor-epic-acceptance.yml", "utf8")
  assert.match(source, /^  workflow_dispatch:$/mu)
  assert.doesNotMatch(source, /^  (?:pull_request|push):/mu)
  assert.match(source, /EXPECTED_SOURCE_SHA: \$\{\{ inputs\.snapshot_sha \}\}/u)
  assert.match(source, /DISPATCH_SOURCE_SHA: \$\{\{ github\.sha \}\}/u)
  assert.match(source, /ref: \$\{\{ inputs\.snapshot_sha \}\}/u)
  assert.match(source, /cancel-in-progress: false/u)
  const marker = "      - name: Require a fixed diagnostic source\n"
  assert.ok(source.indexOf(marker) < source.indexOf("      - uses: actions/checkout@"))
  const block = source.split(marker)[1].split("\n      - ")[0]
  const script = block.split("        run: |\n")[1].replace(/^          /gmu, "")
  const sha = "a".repeat(40)
  for (const [snapshot, dispatched, valid] of [[sha, sha, true], [sha, "b".repeat(40), false],
    ["b".repeat(40), "b".repeat(40), true], ["A".repeat(40), "A".repeat(40), false],
    ["a".repeat(39), sha, false], ["", "", false], [sha + "\n", sha, false], ["$(exit 0)", sha, false]]) {
    const result = spawnSync("bash", ["-c", script], { encoding: "utf8",
      env: { PATH: process.env.PATH, EXPECTED_SOURCE_SHA: snapshot, DISPATCH_SOURCE_SHA: dispatched } })
    assert.equal(result.status === 0, valid, "foreign/malformed diagnostic source must fail closed")
  }
})

test("manual subscription diagnostic retains the original API-only joint recipe", () => {
  const source = readFileSync(".github/workflows/observer-subscriptions.yml", "utf8")
  assertLinuxTestOwnership({ "observer-subscriptions.yml": source })
  for (const [before, after] of [["BAMBOO_FRONTEND_BUILD_MODE: api-only", "BAMBOO_FRONTEND_BUILD_MODE: embedded"],
    ["ref: ${{ github.sha }}", "ref: dev"],
    ["EXPECTED_SOURCE_SHA: ${{ github.sha }}", "EXPECTED_SOURCE_SHA: dev"],
    ["ubuntu-latest", "windows-latest"], ["  workflow_dispatch:", "  pull_request:\n    branches: [dev]"],
    ["-p bamboo-domain -p bamboo-engine -p bamboo-server --lib", "-p bamboo-server --lib"],
    ["observer_subscription session::supervisor_management::tests::", "observer_subscription"],
    ["--test-threads=1", "--test-threads=2"],
    ["assert len(window) == 6", "assert len(window) >= 0"]]) {
    assert.ok(source.includes(before), before)
    assert.throws(() => assertLinuxTestOwnership({ "observer-subscriptions.yml": source.replace(before, after) }))
  }
  for (const name of OBSERVER_FAULT_HOST_CASES) {
    assert.ok(source.includes(name), name)
    assert.throws(() => assertLinuxTestOwnership({ "observer-subscriptions.yml": source.replace(name, name + "_unowned") }))
  }
  assert.throws(() => assertLinuxTestOwnership({ "unregistered-subscriptions.yml": source.replace("  workflow_dispatch:", "  pull_request:\n    branches: [dev]") }), /must join the canonical feature executor/u)
})

function serverCommand(id) {
  const name = id === "test-crates" ? "Test server block" : "Run E2E server block"
  const step = job(id, historical.ci).split(`      - name: ${name}\n`)[1]?.split(/\n      - /u)[0]
  const command = step?.match(/        run: \|\n([\s\S]*)/u)?.[1].replace(/^          /gmu, "")
  assert.ok(command)
  return command
}

function serverCargoFixture(t) {
  const directory = mkdtempSync(path.join(os.tmpdir(), "bamboo-server-cargo-"))
  t.after(() => rmSync(directory, { recursive: true, force: true }))
  const calls = path.join(directory, "calls")
  writeFileSync(path.join(directory, "cargo"), `#!/usr/bin/env node
const fs = require('node:fs')
const args = process.argv.slice(2)
fs.appendFileSync(process.env.CARGO_CALLS, JSON.stringify(args) + '\\n')
const kind = args.includes('--doc') ? 'docs' : args[0] === 'build' ? 'examples' : args.includes('--lib') ? 'library' : 'integration'
process.exit(process.env.TEST_FAIL_KIND === kind ? 17 : 0)
`, { mode: 0o755 })
  return (id, block, failure = "") => {
    writeFileSync(calls, "")
    const result = spawnSync("bash", ["-e", "-o", "pipefail", "-c", serverCommand(id)], {
      encoding: "utf8", env: { ...process.env, PATH: `${directory}:${process.env.PATH}`,
        SERVER_TEST_BLOCK: block, CARGO_CALLS: calls, TEST_FAIL_KIND: failure },
    })
    return { ...result, calls: readFileSync(calls, "utf8").trim().split("\n").filter(Boolean).map(JSON.parse) }
  }
}

legacyTest("server library commands partition every nested namespace by its first matching filter", (t, workflow, job, contributing) => {
  const run = serverCargoFixture(t)
  const names = ["future::unclassified::tests::case", ...serverFilters.map((filter) => `future::${filter}tests::case`)]
  for (const first of serverFilters) for (const second of serverFilters) {
    names.push(`future::${first}nested::${second}tests::case`)
  }
  for (const id of ["test-crates", "e2e-crates"]) {
    const predicates = serverBlocks.slice(0, -1).map((block, index) => {
      const result = run(id, block)
      assert.equal(result.status, 0, result.stderr)
      assert.equal(result.calls.length, 1)
      const args = result.calls[0]
      const separator = args.indexOf("--")
      assert.ok(separator > 0)
      assert.deepEqual(args.slice(0, 5), ["test", "--locked", "--no-fail-fast", "-p", "bamboo-server"])
      assert.equal(args.includes("--all-features"), id === "e2e-crates")
      const include = index < serverFilters.length ? serverFilters[index] : undefined
      assert.deepEqual(args.slice(args.indexOf("--lib") + 1, separator), include ? [include] : [])
      assert.deepEqual(args.slice(separator + 1), serverFilters.slice(0, index).flatMap((filter) => ["--skip", filter]))
      return (name) => (!include || name.includes(include)) && serverFilters.slice(0, index).every((filter) => !name.includes(filter))
    })
    for (const name of names) assert.equal(predicates.filter((predicate) => predicate(name)).length, 1, name)
    assert.equal(run(id, "browser", "library").status, 17)
    const unknown = run(id, "unknown")
    assert.notEqual(unknown.status, 0)
    assert.deepEqual(unknown.calls, [])
  }
})

legacyTest("server Cargo target layout keeps the unfiltered integration tail complete", (t, workflow, job, contributing) => {
  const root = "crates/app/bamboo-server"
  const python = ["python3", "python3.12", "python3.14"].find((candidate) => spawnSync(candidate, ["-c", "import tomllib"]).status === 0)
  assert.ok(python, "Cargo target policy requires Python with tomllib")
  const parsed = spawnSync(python, ["-c", "import json,tomllib; print(json.dumps(tomllib.load(open('crates/app/bamboo-server/Cargo.toml','rb'))))"], { encoding: "utf8" })
  assert.equal(parsed.status, 0, parsed.stderr)
  const manifest = JSON.parse(parsed.stdout)
  assert.notEqual(manifest.package.autotests, false)
  assert.notEqual(manifest.package.autolib, false)
  assert.notEqual(manifest.package.autoexamples, false)
  assert.deepEqual(manifest.lib ?? {}, {})
  assert.deepEqual(manifest.bin ?? [], [])
  assert.deepEqual(manifest.example ?? [], [])
  assert.deepEqual(manifest.bench ?? [], [])
  assert.deepEqual(manifest.test, [{ name: "env_cache_isolation", path: "tests/env_cache_isolation.rs", "required-features": ["test-utils"] }])
  assert.deepEqual(manifest.features.default ?? [], [])
  assert.ok(manifest.features["test-utils"])
  assert.equal(existsSync(path.join(root, "src/main.rs")), false)
  assert.equal(existsSync(path.join(root, "src/bin")), false)
  const entries = readdirSync(path.join(root, "tests"), { withFileTypes: true })
  for (const entry of entries.filter((entry) => entry.isDirectory())) {
    assert.equal(existsSync(path.join(root, "tests", entry.name, "main.rs")), false, "directory targets need an explicit selector review")
  }
  const targets = entries.filter((entry) => entry.isFile() && entry.name.endsWith(".rs")).map((entry) => entry.name.slice(0, -3)).sort()
  assert.ok(targets.includes("env_cache_isolation"))
  const run = serverCargoFixture(t)
  const unit = run("test-crates", "integration")
  assert.equal(unit.status, 0, unit.stderr)
  assert.equal(unit.calls.length, 3)
  const base = ["test", "--locked", "--no-fail-fast", "-p", "bamboo-server"]
  assert.deepEqual(unit.calls[0], [...base, ...targets.filter((name) => name !== "env_cache_isolation").flatMap((name) => ["--test", name])])
  assert.deepEqual(unit.calls[1], ["build", "--locked", "-p", "bamboo-server", "--examples", "--profile", "test"])
  assert.deepEqual(unit.calls[2], [...base, "--doc"])
  const e2e = run("e2e-crates", "integration")
  assert.equal(e2e.status, 0, e2e.stderr)
  assert.deepEqual(e2e.calls, [[...base, "--all-features", "--test", "*"]])
  for (const failure of ["integration", "examples", "docs"]) {
    const failed = run("test-crates", "integration", failure)
    assert.equal(failed.status, 17)
    assert.equal(failed.calls.length, 3, "a failure must not skip the other default target kinds")
  }
  assert.equal(run("e2e-crates", "integration", "integration").status, 17)
})

legacyTest("native summaries reject failed, cancelled, skipped and missing crate results", (_, workflow, job, contributing) => {
  for (const [id, stepName, variables] of [
    ["test", "Require all crate tests and the SSH fixture", ["CRATE_RESULT", "E2E_RESULT", "SSH_RESULT"]],
    ["e2e-test", "Require all E2E crate tests", ["CRATE_RESULT", "SSH_RESULT"]],
  ]) {
    const step = job(id).split(`      - name: ${stepName}\n`)[1]?.split(/\n      - /u)[0]
    assert.ok(step)
    const command = step.match(/        run: \|\n([\s\S]*)/u)?.[1].replace(/^          /gmu, "")
      ?? step.match(/        run: ([^\n]+)/u)?.[1]
    assert.ok(command)
    const results = Object.fromEntries(variables.map((name) => [name, "success"]))
    const run = (overrides = {}) => spawnSync("bash", ["-e", "-o", "pipefail", "-c", command], {
      env: { ...process.env, SOURCE_RESULT: "success", FULL_VALIDATION: "true", FEATURE_VALIDATION: "false",
        LINT_RESULT: "success", MSRV_RESULT: "success", SECURITY_RESULT: "success", DENY_RESULT: "success", DOCS_RESULT: "success",
        ...results, ...overrides }, encoding: "utf8",
    })
    assert.equal(run().status, 0)
    for (const variable of variables) {
      for (const result of ["failure", "cancelled", "skipped", "", "unknown"]) {
        assert.notEqual(run({ [variable]: result }).status, 0, `${id}: ${variable}=${result}`)
      }
    }
  }
})

legacyTest("promotion retains required checks without repeating platform coverage", (_, workflow, job, contributing) => {
  assert.match(job("promotion-source"), /name: Promotion Source\n/u)
  assert.match(job("lint"), /name: Lint\n/u)

  const e2e = job("e2e-test")
  assert.match(e2e, /name: E2E Tests\n/u)
  assert.match(e2e, /test "\$CRATE_RESULT" = success/u)
  assert.match(job("e2e-crates"), /--all-features --lib --tests/u)
  assert.doesNotMatch(e2e, /cargo test/u)
  assert.doesNotMatch(workflow, /cargo test --test e2e_tests --all-features/u)

  const tls = job("tls-fixture-test")
  assert.match(tls, /os: \[macos-latest, windows-latest\]/u)
  assert.match(tls, /if: runner\.os == 'macOS'\n        run: scripts\/run-macos-server-lib-tests\.sh/u)
  assert.match(tls, /if: runner\.os == 'Windows'\n        run: cargo test --locked --no-fail-fast -p bamboo-server --lib server::tls::tests/u)

  const build = job("build")
  assert.match(build, /name: Platform Tests \(\$\{\{ matrix\.os \}\}\)\n/u)
  assert.match(build, /os: \[ubuntu-latest, macos-latest, windows-latest\]/u)
  assert.doesNotMatch(workflow, /cargo build[^\n]*--release/u)
  const sourceStep = build.indexOf("- name: Verify portable Skill source publication")
  const selectedStep = build.indexOf("- name: Verify portable selected Skill source")
  const readToolStep = build.indexOf("- name: Verify portable Skill read Tool")
  const frontendStep = build.indexOf("- name: Verify portable frontend package contract")
  assert.ok(sourceStep >= 0 && selectedStep > sourceStep && frontendStep > selectedStep)
  assert.match(build, /run: cargo test --locked --no-fail-fast -p bamboo-skills progressive::source_tests -- --test-threads=1\n/u)
  assert.match(build, /run: cargo test --locked --no-fail-fast -p bamboo-skills progressive::read_tests -- --test-threads=1\n/u)
  assert.equal((workflow.match(/progressive::read_tests/gu) || []).length, 1)
  assert.ok(readToolStep > selectedStep && readToolStep < frontendStep)
  assert.match(build, /run: cargo test --locked --no-fail-fast -p bamboo-server-tools skill_runtime::catalog_tests -- --test-threads=1\n/u)
  assert.equal((workflow.match(/skill_runtime::catalog_tests/gu) || []).length, 1)
  assert.doesNotMatch(job("test"), /Verify portable Skill read Tool/u)

  assert.match(
    build,
    /- name: Test frontend artifact and release policies\n        if: runner\.os != 'Linux'\n        run: node --test scripts\/lotus-next-artifact.test.cjs scripts\/release-frontend-policy.test.cjs\n/u,
  )
  assert.match(
    build,
    /- name: Verify portable frontend package contract\n        if: runner\.os != 'Linux'\n        run: cargo test --locked --no-fail-fast -p bamboo-server --test frontend_build_contract\n/u,
  )
})

legacyTest("all-feature E2E crates retain the workspace stack and required summary", (_, workflow, job, contributing) => {
  assert.match(workflow, /^  RUST_MIN_STACK: "8388608"$/mu)
  assert.match(job("e2e-crates"), /--all-features --lib --tests/u)
  assert.match(job("e2e-test"), /needs: \[validation-source, feature-tests, native-feature-tests, browser-runtime-tests, lint, msrv, security, deny, docs, feature-codeql, frontend-inventory, e2e-crates, test-ssh\]/u)
  assert.match(job("e2e-test"), /always\(\) && !cancelled\(\)/u)
})

legacyTest("ordinary PR caches remain reusable and frozen validation restores without saving", (_, workflow, job, contributing) => {
  assert.ok(job("test").includes(cacheSaveIf(
    "github.event_name == 'push' || (github.event_name == 'pull_request' && github.base_ref == 'dev')",
  )))
  for (const id of [
    "msrv",
    "tls-fixture-test",
    "tool-event-recorder-e2e",
    "e2e-crates",
    "test-crates",
    "test-ssh",
    "lint",
    "docs",
    "security",
  ]) {
    assert.ok(
      job(id).includes(cacheSaveIf("github.event_name == 'push' && github.ref == 'refs/heads/main'")),
      `${id} must restore but not write PR-scoped caches`,
    )
  }
  assert.match(job("build"), /save-if: .*github\.base_ref == 'main'/u)
  assert.doesNotMatch(workflow, /cache-on-failure: true/u)

  assert.match(cacheCleanupWorkflow, /pull_request:\n    branches: \[ dev, main \]\n    types: \[ closed \]/u)
  assert.match(
    cacheCleanupWorkflow,
    /if: github\.event\.pull_request\.head\.repo\.full_name == github\.repository/u,
  )
  assert.match(cacheCleanupWorkflow, /permissions:\n      actions: write/u)
  assert.match(
    cacheCleanupWorkflow,
    /gh cache delete --all --ref "refs\/pull\/\$\{PR_NUMBER\}\/merge" --succeed-on-no-caches/u,
  )
  assert.doesNotMatch(cacheCleanupWorkflow, /actions\/checkout|pull_request_target/u)
})

legacyTest("legacy release jobs retain explicit full-validation selection", (_, workflow, job, contributing) => {
  for (const id of [
    "msrv",
    "tls-fixture-test",
    "tool-event-recorder-e2e",
    "e2e-test",
    "lint",
    "docs",
    "security",
    "deny",
  ]) {
    assert.ok(job(id).includes("needs.validation-source.outputs.full == 'true'"), `${id} must be comprehensive-only`)
  }
})

legacyTest("main promotion and manual native platform test policy stays intact", (_, workflow, job, contributing) => {
  const promotion = job("promotion-source")
  assert.match(promotion, /github\.base_ref == 'main'/u)
  assert.match(promotion, /HEAD_REF.*github\.head_ref/u)

  const build = job("build")
  assert.match(build, /github\.event_name == 'workflow_dispatch'/u)
  assert.match(build, /github\.base_ref == 'main'/u)
  assert.match(build, /startsWith\(github\.head_ref, 'release\/'\)/u)
  assert.match(build, /os: \[ubuntu-latest, macos-latest, windows-latest\]/u)
})

test("CodeQL stays on feature and manual paths without compiling Rust", () => {
  assert.match(codeqlWorkflow, /workflow_dispatch:\n/u)
  assert.match(codeqlWorkflow, /pull_request:\n\s+branches: \[ dev \]/u)
  assert.doesNotMatch(codeqlWorkflow, /push:|branches: \[[^\]]*main/u)
  assert.match(codeqlWorkflow, /language: \[ actions, javascript-typescript, python, rust \]/u)
  assert.match(codeqlWorkflow, /build-mode: none/u)
})

const promotionScript = () => {
  const block = job("promotion-source", historical.ci).match(/        run: \|\n([\s\S]*)$/u)
  assert.ok(block, "source proof must stay inline before any checkout")
  return block[1].split("\n").map((line) => line.replace(/^          /u, "")).join("\n")
}

legacyTest("frozen promotion bootstrap is read-only and leaves merge-tree validation intact", (_, workflow, job, contributing) => {
  const promotion = job("promotion-source")
  assert.match(promotion, /permissions:\n      contents: read\n/u)
  assert.equal((promotion.match(/^      - /gmu) || []).length, 1)
  assert.doesNotMatch(promotion, /uses:|environment:|continue-on-error:|contents: write|pull_request_target/u)
  for (const binding of [
    "GH_TOKEN: ${{ github.token }}",
    "HEAD_REF: ${{ github.head_ref }}",
    "HEAD_SHA: ${{ github.event.pull_request.head.sha }}",
    "HEAD_REPOSITORY: ${{ github.event.pull_request.head.repo.full_name }}",
  ]) assert.ok(promotion.includes(binding), `missing event binding: ${binding}`)
  const bootstrap = promotionScript()
  assert.doesNotMatch(bootstrap.replace(/^#.*$/gmu, ""), /\$\{\{|subprocess|eval\(|exec\(|checkout|https?:\/\/(?!api\.github\.com\/)/u)
  assert.match(bootstrap, /class NoRedirect\(urllib\.request\.HTTPRedirectHandler\):/u)
  assert.match(bootstrap, /urllib\.request\.build_opener\(NoRedirect\(\)\)/u)
  assert.match(bootstrap, /opener\.open\(request, timeout=30\)/u)
  // No source checkout is pinned over the PR's synthetic merge result.
  for (const id of ["test", "test-crates", "e2e-crates", "test-ssh", "lint", "build"]) {
    assert.match(job(id), /- uses: actions\/checkout@v7\n(?:        if: github.event_name != 'push'\n)?/u)
  }
  for (const context of ["Promotion Source", "Test", "Lint", "E2E Tests", "Platform Tests (${{ matrix.os }})"]) {
    assert.ok(workflow.includes(`name: ${context}\n`), `retain required context: ${context}`)
  }
  assert.match(contributing, /dev → release\/<40-character-cut-SHA> → main/u)
  assert.match(contributing, /freshly read its actual current HEAD immediately before/u)
  assert.match(contributing, /UTC timestamp, full lowercase\n   40-character SHA, tree SHA, fresh branch-read receipt/u)
  assert.match(contributing, /Static CI cannot prove the historical timing/u)
  assert.doesNotMatch(contributing, /it need not be\s+the current tip/u)
  assert.match(contributing, /do not squash or rebase/u)
  assert.match(contributing, /original fix head `F` within the recorded release scope/u)
  assert.match(contributing, /integration head `U` containing both\n   `F` and the current protected `dev` head/u)
  assert.match(contributing, /exact `F` → `U` → accepted-`dev` ancestry/u)
  assert.match(contributing, /Bind both receipts to their respective\n   SHA\/tree/u)
  assert.match(contributing, /branch to the recorded original fix head `F`; never substitute `U`/u)
  assert.match(contributing, /dev merge result for recorded `F`\. Review must reject any other repair target/u)
  assert.match(contributing, /Pending or failing release-source or required integration checks block this/u)
  assert.match(contributing, /Only if existing branch rules cannot preserve exact\n   `F` ancestry, or the necessary repair cannot stay within the recorded scope/u)
  assert.match(contributing, /this gate alone cannot\n   authorize `U` as the release target/u)
  assert.match(contributing, /Release source and version tags are immutable/u)
  assert.match(contributing, /known pending frontend consumer awaits its immutable producer/u)
  assert.match(contributing, /Do not describe this known dependency as an unexpected fix/u)
  assert.match(contributing, /separate `F` release-source and `U` protected-dev integration procedure/u)
  assert.match(contributing, /independent Code and Security review and run affected local tests on that\n   exact `F`/u)
  assert.match(contributing, /Run the complete release suite and Linux\/macOS\/Windows matrix on the final\n   release PR before promotion/u)
  for (const source of [contributing, historical.gitflowLegacy]) {
    assert.match(source, /manual full CI on `F`[\s\S]*?optional diagnosis,[\s\S]*?not a second mandatory\s+full run/u)
  }
  assert.match(contributing, /This exception changes ordering only, not gates,\n   protection, publication authority or the `Promotion Source` contract/u)
  assert.match(readFileSync("AGENTS.md", "utf8"), /CONTRIBUTING\.md#release-process/u)
})

test("frozen promotion proves exact ancestry, accepts dev advancement, and rejects missing or changed proof", () => {
  const directory = mkdtempSync(path.join(os.tmpdir(), "bamboo-frozen-promotion-"))
  const git = (...args) => {
    const result = spawnSync("git", args, { cwd: directory, encoding: "utf8" })
    assert.equal(result.status, 0, result.stderr)
    return result.stdout.trim()
  }
  let sequence = 0
  const commit = (label, file = `${++sequence}.txt`) => {
    writeFileSync(path.join(directory, file), `${label}\n`)
    git("add", ".")
    git("commit", "--quiet", "-m", label)
    return git("rev-parse", "HEAD")
  }
  try {
    git("init", "--quiet")
    git("config", "user.name", "ci-policy")
    git("config", "user.email", "ci-policy@example.invalid")
    commit("root")
    const cut = commit("accepted cut")
    git("switch", "--quiet", "-c", "dev")
    const featureFile = "unrelated-dev-feature.txt"
    const feature = commit("unrelated ongoing development", featureFile)
    git("branch", `release/${cut}`, cut)
    git("switch", "--quiet", "-c", "original-release-fix", cut)
    const fixFile = "bounded-release-fix.txt"
    const fix = commit("bounded release fix", fixFile)
    const pending = commit("not yet backmerged")
    const isAncestor = (source, target) => {
      const result = spawnSync("git", ["merge-base", "--is-ancestor", source, target], {
        cwd: directory, encoding: "utf8",
      })
      assert.ok([0, 1].includes(result.status), result.stderr)
      return result.status === 0
    }
    assert.equal(isAncestor(feature, fix), false, "original F is not up to date with dev D1")
    git("switch", "--quiet", "-c", "strict-dev-integration", feature)
    git("merge", "--quiet", "--no-ff", "-m", "integration U preserves dev D1 and original F", fix)
    const integration = git("rev-parse", "HEAD")
    assert.equal(isAncestor(feature, integration), true, "U contains current dev for strict checks")
    assert.equal(isAncestor(fix, integration), true, "U preserves exact original F ancestry")
    git("switch", "--quiet", "dev")
    // Model only the strict up-to-date ancestry prerequisite. This local merge
    // does not claim live protected admission or actual review/check receipts.
    assert.equal(isAncestor(git("rev-parse", "dev"), integration), true)
    git("merge", "--quiet", "--no-ff", "-m", "model normal admission of reviewed U", integration)
    const reviewed = git("rev-parse", "HEAD")
    const advanced = commit("later dev work")

    // Equal patches/trees do not substitute for the original F's ancestry.
    git("switch", "--quiet", "-c", "cherry-pick-counterexample", feature)
    git("cherry-pick", "--quiet", fix)
    const cherryPicked = git("rev-parse", "HEAD")
    git("switch", "--quiet", "-c", "squash-counterexample", feature)
    git("merge", "--quiet", "--squash", fix)
    git("commit", "--quiet", "-m", "squashed fix counterexample")
    const squashed = git("rev-parse", "HEAD")
    for (const replacement of [cherryPicked, squashed]) {
      assert.equal(isAncestor(fix, replacement), false)
      assert.equal(git("rev-parse", `${replacement}^{tree}`), git("rev-parse", `${integration}^{tree}`))
    }

    // This models the documented review contract, not a new production gate.
    // F's release-source receipt and U's integration receipt are separate.
    const sourceReceipt = { sha: fix, tree: git("rev-parse", `${fix}^{tree}`) }
    const integrationReceipt = { sha: integration, tree: git("rev-parse", `${integration}^{tree}`), acceptedDev: reviewed }
    const requireRecordedRepairTarget = (target) => {
      assert.equal(target, sourceReceipt.sha, "review must require the recorded original fix head F")
      assert.equal(git("rev-parse", `${target}^{tree}`), sourceReceipt.tree)
      assert.equal(git("rev-parse", `${integrationReceipt.sha}^{tree}`), integrationReceipt.tree)
      assert.equal(isAncestor(cut, target), true)
      assert.equal(isAncestor(feature, integrationReceipt.sha), true)
      assert.equal(isAncestor(target, integrationReceipt.sha), true)
      assert.equal(isAncestor(integrationReceipt.sha, integrationReceipt.acceptedDev), true)
    }
    requireRecordedRepairTarget(fix)
    for (const wrongTarget of [integration, reviewed, cherryPicked, squashed, pending]) {
      assert.throws(() => requireRecordedRepairTarget(wrongTarget), /recorded original fix head F/u)
    }
    git("switch", "--quiet", `release/${cut}`)
    git("merge", "--quiet", "--ff-only", fix)
    assert.equal(git("rev-parse", "HEAD"), fix, "release advances only to original F")
    assert.equal(isAncestor(feature, fix), false, "unrelated D1 stays outside the frozen release")
    assert.equal(git("ls-tree", "--name-only", fix).split("\n").includes(featureFile), false)
    assert.equal(git("ls-tree", "--name-only", integration).split("\n").includes(featureFile), true)
    assert.equal(isAncestor(fix, advanced), true, "later dev advancement retains accepted F")
    git("switch", "--quiet", "dev")

    const bootstrap = promotionScript().match(/^python3 - <<'PYTHON'\n([\s\S]*)\nPYTHON\s*$/u)?.[1]
    assert.ok(bootstrap, "extract the exact production Python bootstrap")
    writeFileSync(path.join(directory, "bootstrap.py"), bootstrap)
    // Execute the production bootstrap unchanged. Only the HTTPS transport is
    // replaced; comparison results come from this real temporary Git DAG.
    const fixtureProgram = String.raw`
import io
import json
import os
import re
import runpy
import subprocess
import urllib.error
import urllib.parse
import urllib.request

with open("fixture.json") as stream:
    fixture = json.load(stream)
counts = {"release": 0, "dev": 0, "compare": 0}

def git(*args):
    return subprocess.check_output(["git", *args], text=True).strip()

class FixtureOpener:
    def open(self, request, timeout):
        assert timeout == 30
        assert request.get_method() == "GET"
        assert request.get_header("Authorization") == "Bearer fixture-read-only"
        prefix = "https://api.github.com/repos/bigduu/Bamboo-agent/"
        assert request.full_url.startswith(prefix), request.full_url
        route = request.full_url.removeprefix(prefix)
        if fixture.get("apiFailure"):
            raise urllib.error.HTTPError(request.full_url, 403, "fixture denied", {}, None)
        if fixture.get("redirect"):
            raise urllib.error.HTTPError(request.full_url, 302, "fixture redirect", {"Location": "https://invalid.example/"}, None)
        if route.startswith("branches/"):
            name = urllib.parse.unquote(route.removeprefix("branches/"))
            key = "dev" if name == "dev" else "release"
            if key == "release":
                assert re.fullmatch(r"branches/release%2F[0-9a-f]{40}", route)
            index = counts[key]
            counts[key] += 1
            sha = fixture[key][min(index, len(fixture[key]) - 1)]
            data = {"name": name, "commit": {"sha": sha}, "protected": True}
            if key == "dev":
                protections = fixture.get("protected", [True, True])
                data["protected"] = protections[min(index, len(protections) - 1)]
            data.update(fixture.get(key + "Metadata", {}))
        else:
            match = re.fullmatch(r"compare/([0-9a-f]{40})\.\.\.([0-9a-f]{40})", route)
            assert match, route
            source, target = match.groups()
            counts["compare"] += 1
            merge_base = git("merge-base", source, target)
            status = "identical" if source == target else "ahead" if merge_base == source else "behind" if merge_base == target else "diverged"
            data = {"status": status, "base_commit": {"sha": source}, "merge_base_commit": {"sha": merge_base}}
            data.update(fixture.get("comparison", {}))
        return io.BytesIO(json.dumps(data).encode())

def build_opener(*handlers):
    assert len(handlers) == 1
    handler = handlers[0]
    assert isinstance(handler, urllib.request.HTTPRedirectHandler)
    assert type(handler) is not urllib.request.HTTPRedirectHandler
    assert handler.redirect_request(None, None, 302, "redirect", {}, "https://invalid.example/") is None
    return FixtureOpener()

urllib.request.build_opener = build_opener
runpy.run_path("bootstrap.py", run_name="__main__")
assert counts == fixture.get("counts", {"release": 2, "dev": 2, "compare": 2}), counts
`
    const run = ({ env = {}, ...fixture } = {}) => {
      writeFileSync(path.join(directory, "fixture.json"), JSON.stringify({
        release: [fix, fix], dev: [reviewed, reviewed], ...fixture,
      }))
      return spawnSync("python3", ["-c", fixtureProgram], {
        cwd: directory, encoding: "utf8", env: { ...process.env,
          GITHUB_REPOSITORY: "bigduu/Bamboo-agent", GH_TOKEN: "fixture-read-only",
          HEAD_REPOSITORY: "bigduu/Bamboo-agent", HEAD_REF: `release/${cut}`, HEAD_SHA: fix,
          ...env,
        },
      })
    }
    for (const [name, fixture] of [
      ["original F admitted through strict-compatible U excludes unrelated dev work", {}],
      ["original cut remains accepted after dev advances", { release: [cut, cut], env: { HEAD_SHA: cut } }],
      ["recorded early cut remains valid while a known consumer F awaits admission and dev progresses", { release: [cut, cut], dev: [feature, feature], env: { HEAD_SHA: cut } }],
      ["later dev tip", { dev: [advanced, advanced] }],
      ["API origin cannot be changed by environment", { env: { GITHUB_API_URL: "https://invalid.example" } }],
      ["dev advances between metadata reads", { dev: [reviewed, advanced], counts: { release: 2, dev: 2, compare: 3 } }],
      ["dev and release head may initially be identical", { dev: [fix, fix] }],
    ]) {
      const result = run(fixture)
      assert.equal(result.status, 0, `${name}: ${result.stderr}`)
      assert.match(result.stdout, /Accepted frozen release/u)
    }
    // U also passes the ancestry-only controller after admission. It remains
    // an invalid repair target under the separately required review contract;
    // never present this scope decision as an automatic controller guarantee.
    const integrationAsRelease = run({ release: [integration, integration], env: { HEAD_SHA: integration } })
    assert.equal(integrationAsRelease.status, 0, integrationAsRelease.stderr)
    assert.throws(() => requireRecordedRepairTarget(integration), /recorded original fix head F/u)

    for (const [name, fixture] of [
      ["original F not yet admitted through U", { dev: [feature, feature] }],
      ["cherry-picked equivalent lacks original F ancestry", { dev: [cherryPicked, cherryPicked] }],
      ["squashed equivalent lacks original F ancestry", { dev: [squashed, squashed] }],
      ["fork", { env: { HEAD_REPOSITORY: "someone/Bamboo-agent" } }],
      ["unexpected target repository", { env: { GITHUB_REPOSITORY: "someone/Bamboo-agent", HEAD_REPOSITORY: "someone/Bamboo-agent" } }],
      ["direct dev", { env: { HEAD_REF: "dev" } }],
      ["malformed cut", { env: { HEAD_REF: "release/latest" } }],
      ["short cut", { env: { HEAD_REF: `release/${cut.slice(0, 12)}` } }],
      ["uppercase cut", { env: { HEAD_REF: `release/${cut.toUpperCase()}` } }],
      ["wrong cut ancestor", { env: { HEAD_REF: `release/${feature}` } }],
      ["missing head", { env: { HEAD_SHA: "" } }],
      ["missing token", { env: { GH_TOKEN: "" } }],
      ["release head mismatches PR event", { release: [pending, pending] }],
      ["release moves during proof", { release: [fix, pending] }],
      ["unbackmerged release commit", { release: [pending, pending], env: { HEAD_SHA: pending } }],
      ["dev rewrites to history that excludes the release fix", { dev: [reviewed, feature], counts: { release: 2, dev: 2, compare: 3 } }],
      ["dev rewrites even though the release head remains an ancestor", { dev: [reviewed, pending], counts: { release: 2, dev: 2, compare: 3 } }],
      ["dev initially unprotected", { protected: [false, false] }],
      ["dev loses protection during proof", { protected: [true, false] }],
      ["truthy protection is insufficient", { protected: ["true", "true"] }],
      ["missing dev protection", { devMetadata: { protected: null } }],
      ["branch identity mismatch", { releaseMetadata: { name: "dev" } }],
      ["invalid branch SHA", { devMetadata: { commit: { sha: "dev" } } }],
      ["missing ancestry identity", { comparison: { merge_base_commit: {} } }],
      ["wrong base identity", { comparison: { base_commit: { sha: pending } } }],
      ["unproven ancestry status", { comparison: { status: "unknown" } }],
      ["API failure", { apiFailure: true }],
      ["API redirect", { redirect: true }],
    ]) {
      const result = run(fixture)
      assert.notEqual(result.status, 0, `reject ${name}: ${result.stdout}`)
      assert.doesNotMatch(result.stdout, /Accepted frozen release/u, name)
      assert.doesNotMatch(result.stderr, /fixture-read-only/u, "do not expose the credential on failure")
    }
  } finally {
    rmSync(directory, { recursive: true, force: true })
  }
})

// Keep one generic frontend controller. Every run is bound to its actual event
// head; both profile gates remain required without sharing compiled products.
const frontendWorkflow = readFileSync(
  ".github/workflows/frontend-acceptance.yml", "utf8",
)
test("this frozen batch cannot save cache on source, integration, promotion or main push", () => {
  const actor = readFileSync(".github/workflows/actor-snapshot-windows.yml", "utf8")
  const docs = readFileSync(".github/workflows/docs.yml", "utf8")
  const sources = [workflow, frontendWorkflow, actor]
  assert.doesNotMatch(docs, /rust-cache|cargo doc/u)
  const header = frozenCacheWriteHeader.join("\n")
  const expression = frozenCacheWriteHeader.slice(1).join("\n").trim().slice(3, -2)
  const cutRef = "release/da348a63f1d019a22df547c78575c500b6b1294e"
  const anchor = "dbafb74522794012fc8f0a61e761817b3913989a"
  const context = ({ event = "workflow_dispatch", ref = "", head = "", base = "", before = "", message = "" }) => ({
    event_name: event, ref, head_ref: head, base_ref: base,
    event: { before, head_commit: { message } },
  })
  const cases = [
    ["original source", context({ ref: "refs/heads/bamboo/release/da348-cache-readonly" }), false],
    ["fixed release source", context({ ref: `refs/heads/${cutRef}` }), false],
    ["dev integration", context({ event: "pull_request", ref: "refs/pull/2037/merge", head: "bamboo/release/da348-main-ancestry", base: "dev" }), false],
    ["main promotion", context({ event: "pull_request", ref: "refs/pull/2036/merge", head: cutRef, base: "main" }), false],
    ["first promotion push", context({ event: "push", ref: "refs/heads/main", before: anchor }), false],
    ["identified later promotion push", context({ event: "push", ref: "refs/heads/main", before: "changed-base", message: `Merge pull request #2036 from bigduu/${cutRef}` }), false],
    ["batch backmerge", context({ event: "pull_request", head: "bamboo/release/da348-main-backmerge", base: "dev" }), false],
    ["case-insensitive branch function", context({ ref: "refs/heads/BAMBOO/RELEASE/DA348-SOURCE" }), false],
    ["other source", context({ ref: "refs/heads/bamboo/release/other-source" }), true],
    ["similar unrelated prefix", context({ ref: "refs/heads/bamboo/release/da348x-source" }), true],
    ["ordinary dev PR", context({ event: "pull_request", head: "feature/example", base: "dev" }), true],
    ["other release PR", context({ event: "pull_request", head: "release/another-cut", base: "main" }), true],
    ["later unrelated main push", context({ event: "push", ref: "refs/heads/main", before: "other-base", message: "ordinary merge" }), true],
  ]
  const evaluate = (value, github, allowed) => runInNewContext(value, {
    github, env: { BAMBOO_RELEASE_CACHE_WRITE_ALLOWED: String(allowed) },
    startsWith: (text, prefix) => String(text).toLowerCase().startsWith(prefix.toLowerCase()),
    contains: (text, part) => String(text).toLowerCase().includes(part.toLowerCase()),
  }, { timeout: 1000 })
  let caches = 0
  for (const source of sources) {
    const actualHeader = source.match(/^  BAMBOO_RELEASE_CACHE_WRITE_ALLOWED: >-\n((?:    .*\n)+)/mu)
    const actualExpression = actualHeader ? actualHeader[1].trim().slice(3, -2) : "true"
    const steps = source.split(/\n      - /u).filter((step) => step.includes("uses: swatinem/rust-cache@v2"))
    for (const step of steps) {
      const conditions = [...step.matchAll(/^          save-if: \$\{\{ (.*) \}\}$/gmu)]
      assert.ok(conditions.length <= 1, "a cache action cannot have conflicting save predicates")
      const saveExpression = conditions.length ? conditions[0][1] : "true"
      caches += 1
      for (const [name, github, expected] of cases) {
        const allowed = evaluate(actualExpression, github, true)
        if (!expected) assert.equal(evaluate(saveExpression, github, allowed), false, `${name} must not save cache`)
      }
      assert.equal(conditions.length, 1, "each cache action must have an explicit save predicate")
      assert.ok(saveExpression.includes("env.BAMBOO_RELEASE_CACHE_WRITE_ALLOWED == 'true'"))
    }
    assert.equal(source.split(header).length, 2, "one shared batch predicate must govern each workflow")
    for (const [name, github, expected] of cases) assert.equal(evaluate(expression, github, true), expected, name)
  }
  assert.equal(caches, 8, "all remaining Cargo cache actions retain the frozen batch policy")
  assert.ok(actor.includes(cacheSaveIf("github.event_name == 'pull_request' && github.base_ref == 'dev'")))
  const publisher = readFileSync(".github/workflows/publish-crate.yml", "utf8")
  const nativePublisher = publisher.split('  executables:\n')[1].split('  publish-executables:\n')[0]
  assert.match(nativePublisher, /uses: Swatinem\/rust-cache@[0-9a-f]{40}/u)
  assert.match(nativePublisher, /cache-workspace-crates: false/u)
  assert.match(nativePublisher, /save-if: \$\{\{ steps\.cache_policy\.outputs\.save == 'true' \}\}/u)
  assert.doesNotMatch(publisher.replace(nativePublisher, ''), /(?:[Ss]watinem\/rust-cache|actions\/cache)@|\bcache-to:/u)
})
const frontendHead = "${{ github.event_name == 'pull_request' && github.event.pull_request.head.sha || github.sha }}"
const frontendAcceptanceTargets = [
  "-p bamboo-agent --test frontend_packaging",
  "-p bamboo-server --test frontend_build_contract",
  "-p bamboo-server --lib services::frontend_package::tests",
]
const frontendJobs = (source) => {
  const parts = source.replace(/\r\n/gu, "\n").split("\njobs:\n")
  assert.equal(parts.length, 2, "exactly one jobs block is required")
  const sections = parts[1].replace(/^  #.*(?:\n|$)/gmu, "").split(/^  ([a-z][a-z-]*):\n/gmu)
  assert.equal(sections.length, 7, "exactly the original lint plus build and test jobs are allowed")
  assert.deepEqual([sections[1], sections[3], sections[5]], ["lint", "build", "test"])
  return [sections[4], sections[6]].map((section) => {
    const [settings, ...steps] = section.split(/(?=^      - )/mu)
    return { settings, steps: steps.map((step) => step.trimEnd()) }
  })
}
const frontendStep = (job, name) => {
  const matches = job.steps.filter((step) => step.startsWith(`      - name: ${name}\n`))
  assert.equal(matches.length, 1, `exactly one ${name} step is required`)
  return matches[0]
}
const frontendScript = (step) => {
  const match = step.match(/^        run: \|\n([\s\S]*)$/mu)
  assert.ok(match, "guard must be an executable block")
  return match[1].split("\n").map((line) => line.replace(/^          /u, "")).join("\n")
}
function assertFrontendPolicy(source) {
  source = source.replace(/\r\n/gu, "\n")
  const header = source.slice(0, source.indexOf("\njobs:\n")).split("\n").filter((line) => line.trim() && !line.trimStart().startsWith("#"))
  assert.deepEqual(header, [
    "name: Frontend consumer acceptance",
    "on:",
    "  workflow_dispatch:",
    "    inputs:",
    "      snapshot_sha:",
    "        description: Exact 40-character SHA of the selected ref for source acceptance",
    "        required: true",
    "        type: string",
    "permissions:",
    "  contents: read",
    "env:",
    "  PYTHONDONTWRITEBYTECODE: \"1\"",
    ...frozenCacheWriteHeader,
  ], "manual snapshot routing binds the actual source without Cargo environment overrides")
  const lint = source.split("\n  lint:\n")[1].split("\n  build:\n")[0].replace(/^  #.*(?:\n|$)/gmu, "").trimEnd()
  assert.equal(lint, [
    "    name: Original release Clippy and frontend policy",
    "    runs-on: ubuntu-latest",
    "    steps:",
    "      - uses: actions/checkout@v7",
    "        with:",
    "          ref: ${{ github.event_name == 'pull_request' && github.event.pull_request.head.sha || github.sha }}",
    "",
    "      - name: Require the exact clean candidate source",
    "        env:",
    "          EXPECTED_SNAPSHOT_SHA: ${{ inputs.snapshot_sha }}",
    "          EXPECTED_SHA: ${{ github.event_name == 'pull_request' && github.event.pull_request.head.sha || github.sha }}",
    "        run: |",
    "          set -euo pipefail",
    "          if [[ \"$GITHUB_EVENT_NAME\" == workflow_dispatch ]]; then",
    "            [[ \"$EXPECTED_SNAPSHOT_SHA\" =~ ^[0-9a-f]{40}$ ]]",
    "            test \"$EXPECTED_SNAPSHOT_SHA\" = \"$GITHUB_SHA\"",
    "          fi",
    "          test \"$(git rev-parse HEAD)\" = \"$EXPECTED_SHA\"",
    "          source_status=\"$(git status --porcelain --untracked-files=all)\"",
    "          test -z \"$source_status\"",
    "",
    "      - name: Setup Node.js",
    "        uses: actions/setup-node@v7",
    "        with:",
    "          node-version: lts/*",
    "",
    "      - name: Verify exact committed frontend and release policies",
    "        run: |",
    "          node --test scripts/lotus-next-artifact.test.cjs scripts/release-frontend-policy.test.cjs",
    "          node scripts/frontend-package.cjs verify",
    "",
    "      - name: Install Rust",
    "        uses: dtolnay/rust-toolchain@stable",
    "        with:",
    "          components: rustfmt, clippy",
    "",
    "      - name: Rust cache",
    "        uses: swatinem/rust-cache@v2",
    "        with:",
    "          shared-key: bamboo-ci-lint-${{ runner.os }}",
    "          " + cacheSaveIf("github.event_name == 'workflow_dispatch' || (github.event_name == 'pull_request' && github.base_ref == 'dev')"),
    "",
    "      - name: Check formatting",
    "        run: cargo fmt --all -- --check",
    "",
    "      - name: Run clippy",
    "        run: |",
    "          cargo clippy --all-features -- \\",
    "            -D warnings \\",
    "            -A deprecated \\",
    "            -A clippy::module_inception \\",
    "            -A clippy::incompatible_msrv \\",
    "            -A clippy::needless_borrows_for_generic_args \\",
    "            -A clippy::too-many-arguments \\",
    "            -A clippy::while-let-loop \\",
    "            -A clippy::type_complexity \\",
    "            -A dead-code",
    "",
    "      - name: Require source files to remain unchanged",
    "        run: git diff --exit-code",
  ].join("\n"), "the lint source guards and Clippy arguments remain exact; frozen validation only changes cache saving")
  const jobs = frontendJobs(source)
  source = "  build:\n" + source.split("  build:\n")[1]
  const withoutFinalConditions = source.replaceAll(
    "      - name: Require the exact clean source after execution\n        if: always()\n",
    "      - name: Require the exact clean source after execution\n",
  )
  assert.doesNotMatch(withoutFinalConditions, /^\s*(?:if|continue-on-error|working-directory|defaults|strategy|container|services|secrets|environment):/mu)
  assert.doesNotMatch(source, /(?:actions\/(?:cache|upload-artifact|download-artifact)|rust-cache)@|\b(?:git reset|git clean|git checkout|git switch|cargo install|cargo publish)\b/u)
  assert.doesNotMatch(source, /^\s*permissions:/mu)
  assert.deepEqual([...source.matchAll(/^      - uses: (.*)$/gmu)].map((m) => m[1]), ["actions/checkout@v7", "actions/checkout@v7"])
  assert.deepEqual([...source.matchAll(/^        uses: (.*)$/gmu)].map((m) => m[1]), ["actions/setup-node@v7", "dtolnay/rust-toolchain@stable", "actions/setup-node@v7", "dtolnay/rust-toolchain@stable"])
  for (const [index, frontendJob] of jobs.entries()) {
    assert.ok(frontendJob.settings.startsWith(`    name: Original release ${index === 0 ? "Build" : "Test"} on frozen consumer source\n`))
    assert.doesNotMatch(frontendJob.settings, /^    env:/mu, "job environments must not alter Cargo or test behavior")
    assert.match(frontendJob.settings, new RegExp(`^    runs-on: ubuntu-latest\n    timeout-minutes: ${index === 0 ? 45 : 75}$`, "mu"))
    if (index === 0) {
      assert.doesNotMatch(frontendJob.settings, /^    needs:/mu)
      for (const [name, step, field] of [["source_sha", "source", "sha"], ["source_tree", "source", "tree"], ["rustc_version", "toolchain", "rustc_version"], ["cargo_version", "toolchain", "cargo_version"]]) {
        assert.ok(frontendJob.settings.includes(`      ${name}: \${{ steps.${step}.outputs.${field} }}`))
      }
    } else {
      assert.match(frontendJob.settings, /^    needs: build$/mu)
    }
    assert.equal(frontendJob.steps[0], `      - uses: actions/checkout@v7\n        with:\n          ref: ${frontendHead}\n          persist-credentials: false`)
    const names = frontendJob.steps.slice(1).map((step) => step.match(/^      - name: (.*)$/mu)?.[1])
    assert.deepEqual(names, [
      "Require the exact clean candidate source", "Setup Node.js", "Test original CI policy", "Install Rust",
      "Record and bind the actual Rust toolchain", "Check formatting",
      ...(index === 0 ? ["Build locked Rust workspace"] : ["Compile locked frontend acceptance targets", "Test locked frontend acceptance targets"]),
      "Require source files to remain unchanged",
      "Require the exact clean source after execution",
    ])
    assert.equal(frontendStep(frontendJob, "Setup Node.js"), "      - name: Setup Node.js\n        uses: actions/setup-node@v7\n        with:\n          node-version: lts/*\n          package-manager-cache: false")
    assert.equal(frontendStep(frontendJob, "Install Rust"), "      - name: Install Rust\n        uses: dtolnay/rust-toolchain@stable\n        with:\n          components: rustfmt")
    for (const [name, command, timeout] of [
      ["Test original CI policy", "node --test scripts/ci-policy.test.cjs"],
      ["Check formatting", "cargo fmt --all -- --check"],
      ...(index === 0 ? [["Build locked Rust workspace", "cargo build --locked"]] : [
        ["Compile locked frontend acceptance targets", frontendAcceptanceTargets.map(target => `cargo test --locked --no-run ${target}`).join("\n"), 15],
        ["Test locked frontend acceptance targets", frontendAcceptanceTargets.map(target => `cargo test --locked --no-fail-fast ${target}`).join("\n"), 45],
      ]),
      ["Require source files to remain unchanged", "git diff --exit-code"],
    ]) {
      const run = timeout ? `|\n${command.split("\n").map(line => `          ${line}`).join("\n")}` : command
      assert.equal(frontendStep(frontendJob, name), `      - name: ${name}\n${timeout ? `        timeout-minutes: ${timeout}\n` : ""}        run: ${run}${timeout ? '\n        env:\n          RUST_MIN_STACK: "8388608"' : ""}`)
    }
    assert.equal(frontendStep(frontendJob, "Require the exact clean source after execution"), [
      "      - name: Require the exact clean source after execution", "        if: always()", "        env:",
      `          EXPECTED_SHA: ${frontendHead}`, "          EXPECTED_TREE: ${{ steps.source.outputs.tree }}",
      "        shell: bash", "        run: |",
      "          set -euo pipefail",
      '          source_sha="$(git rev-parse HEAD)"',
      '          source_tree="$(git rev-parse \'HEAD^{tree}\')"',
      '          source_status="$(git status --porcelain --untracked-files=all)"',
      `          printf 'Final source: %s, tree: %s\\n' "$source_sha" "$source_tree"`,
      `          printf '%s\\n' "$source_status"`,
      '          test "$source_sha" = "$EXPECTED_SHA"',
      '          test "$source_tree" = "$EXPECTED_TREE"',
      '          test -z "$source_status"',
    ].join("\n"))
    const guard = frontendStep(frontendJob, "Require the exact clean candidate source")
    assert.equal(guard.split("        run: |\n")[0], "      - name: Require the exact clean candidate source\n        id: source\n        env:\n" +
      "          EXPECTED_SNAPSHOT_SHA: ${{ inputs.snapshot_sha }}\n" +
      `          EXPECTED_SHA: ${frontendHead}\n` + (index === 0 ? "" :
        "          BUILD_SOURCE_SHA: ${{ needs.build.outputs.source_sha }}\n          BUILD_SOURCE_TREE: ${{ needs.build.outputs.source_tree }}\n") + "        shell: bash\n")
    for (const line of ['if [[ "$GITHUB_EVENT_NAME" == workflow_dispatch ]]; then', '  [[ "$EXPECTED_SNAPSHOT_SHA" =~ ^[0-9a-f]{40}$ ]]', '  test "$EXPECTED_SNAPSHOT_SHA" = "$GITHUB_SHA"', 'fi', 'test "$source_sha" = "$EXPECTED_SHA"', 'source_status="$(git status --porcelain --untracked-files=all)"', 'test -z "$source_status"']) {
      assert.ok(frontendScript(guard).split("\n").includes(line), `missing source guard: ${line}`)
    }
    const toolchain = frontendStep(frontendJob, "Record and bind the actual Rust toolchain")
    assert.equal(toolchain.split("        run: |\n")[0], "      - name: Record and bind the actual Rust toolchain\n        id: toolchain\n" + (index === 0 ? "" :
      "        env:\n          BUILD_RUSTC_VERSION: ${{ needs.build.outputs.rustc_version }}\n          BUILD_CARGO_VERSION: ${{ needs.build.outputs.cargo_version }}\n") + "        shell: bash\n")
    for (const line of ['rustc --version --verbose', 'cargo --version --verbose', 'test -n "$rustc_version"', 'test -n "$cargo_version"']) {
      assert.ok(frontendScript(toolchain).split("\n").includes(line))
    }
    if (index === 1) {
      for (const [variable, output, actual] of [["BUILD_SOURCE_SHA", "source_sha", "source_sha"], ["BUILD_SOURCE_TREE", "source_tree", "source_tree"], ["BUILD_RUSTC_VERSION", "rustc_version", "rustc_version"], ["BUILD_CARGO_VERSION", "cargo_version", "cargo_version"]]) {
        const step = variable.startsWith("BUILD_SOURCE") ? guard : toolchain
        assert.ok(step.includes(`          ${variable}: \${{ needs.build.outputs.${output} }}`))
        assert.ok(frontendScript(step).split("\n").includes(`test "$${actual}" = "$${variable}"`))
      }
    }
  }
}

test("frontend acceptance preserves the locked build, focused tests and actual-head identity", () => {
  assert.equal(require("node:fs").existsSync(".github/workflows/frontend-acceptance-recovery.yml"), false,
    "the unused fixed-source recovery controller must not coexist with the generic workflow")
  assertFrontendPolicy(frontendWorkflow)
  for (const [from, to] of [
    ["  workflow_dispatch:", "  workflow_dispatch:\n    inputs:\n      source:\n        type: string"],
    ["  workflow_dispatch:", "  workflow_dispatch:\n  pull_request:\n    branches: [main]"],
    ['PYTHONDONTWRITEBYTECODE: "1"', 'PYTHONDONTWRITEBYTECODE: "0"'],
    ["contents: read", "contents: write"],
    ["permissions:\n", 'env:\n  RUST_TEST_THREADS: "1"\npermissions:\n'],
    ["permissions:\n", 'env:\n  CARGO_PROFILE_TEST_OPT_LEVEL: "3"\npermissions:\n'],
    ["    needs: build", '    env:\n      CARGO_PROFILE_TEST_OPT_LEVEL: "3"\n    needs: build'],
    ["        id: source", '        env:\n          RUST_TEST_THREADS: "1"\n        id: source'],
    [`ref: ${frontendHead}`, "ref: dev"],
    [`ref: ${frontendHead}`, "ref: 1cda5e1dbae79769052300b26d134bd4ecccf9b3"],
    ["needs: build", "needs: []"],
    ["name: Original release Test on frozen consumer source", "name: Different Test"],
    ["timeout-minutes: 45", "timeout-minutes: 60"],
    ["timeout-minutes: 15", "timeout-minutes: 30"],
    ["timeout-minutes: 75", "timeout-minutes: 45"],
    ["name: Test locked frontend acceptance targets\n        timeout-minutes: 45", "name: Test locked frontend acceptance targets\n        timeout-minutes: 30"],
    ["name: Test locked frontend acceptance targets\n        timeout-minutes: 45", "name: Test locked frontend acceptance targets\n        timeout-minutes: 60"],
    ["package-manager-cache: false", "package-manager-cache: true"],
    ["components: rustfmt\n", "toolchain: 1.99.0\n          components: rustfmt\n"],
    ["run: cargo build --locked", "run: true"],
    ["cargo test --locked --no-run -p bamboo-agent --test frontend_packaging\n", "cargo test --locked --no-run\n"],
    ["cargo test --locked --no-fail-fast -p bamboo-agent --test frontend_packaging\n", "cargo test --locked --no-fail-fast\n"],
    ["--test frontend_packaging", "--test ordinary_root_actor_writer"],
    ["--test frontend_build_contract", "--tests"],
    ["--lib services::frontend_package::tests", "--lib"],
    ["cargo test --locked --no-fail-fast -p bamboo-server --test frontend_build_contract", "cargo test --locked --no-fail-fast -p bamboo-server --test frontend_build_contract -- --skip slow"],
    ["cargo test --locked --no-fail-fast -p bamboo-server --lib services::frontend_package::tests", "cargo test --locked --no-fail-fast -p bamboo-server --lib services::frontend_package::tests || true"],
    ['RUST_MIN_STACK: "8388608"', 'RUST_MIN_STACK: "16777216"'],
    ["run: git diff --exit-code", "run: true"],
    ["        if: always()\n", ""],
    ["        if: always()", "        if: success()"],
    ['source_status="$(git status --porcelain --untracked-files=all)"', 'source_status="$(git diff --name-only)"'],
    ['source_status="$(git status --porcelain --untracked-files=all)"', 'source_status="$(git status --porcelain --untracked-files=no)"'],
    ['test -z "$source_status"', "true"],
    ['test "$source_tree" = "$EXPECTED_TREE"', "true"],
    ['test "$source_sha" = "$BUILD_SOURCE_SHA"', "true"],
    ['test "$cargo_version" = "$BUILD_CARGO_VERSION"', "true"],
    ["        run: |\n          cargo test --locked --no-run", "        continue-on-error: true\n        run: |\n          cargo test --locked --no-run"],
    ["    needs: build", "    if: always()\n    needs: build"],
    ["      - name: Check formatting", "      - uses: actions/cache@v4\n      - name: Check formatting"],
    ["      - name: Require source files to remain unchanged", "      - name: Reset source\n        run: git reset --hard\n      - name: Require source files to remain unchanged"],
  ]) {
    assert.ok(frontendWorkflow.includes(from), `mutation target exists: ${from}`)
    assert.throws(() => assertFrontendPolicy(frontendWorkflow.replace(from, to)), `reject unsafe frontend mutation: ${to}`)
  }
})

test("frontend acceptance compile and test commands propagate each target failure", () => {
  const frontendTest = frontendJobs(frontendWorkflow)[1]
  for (const name of ["Compile locked frontend acceptance targets", "Test locked frontend acceptance targets"]) {
    const script = frontendScript(frontendStep(frontendTest, name).split("\n        env:")[0])
    for (const failAt of [0, 1, 2, 3]) {
      // Use the runner's fail-fast shell with a function, never real Cargo.
      const result = spawnSync("bash", ["-e", "-o", "pipefail", "-c", [
        "calls=0",
        "cargo() {",
        "  calls=$((calls + 1))",
        "  printf '%s\\n' \"$*\"",
        "  if [[ $calls == $FAIL_AT ]]; then return 23; fi",
        "  return 0",
        "}",
        script,
      ].join("\n")], { encoding: "utf8", env: { ...process.env, FAIL_AT: String(failAt) } })
      assert.equal(result.status, failAt ? 23 : 0, `${name}: failure at target ${failAt}; ${result.stderr}`)
      assert.equal(result.stdout.trim().split("\n").length, failAt || 3, `${name}: stop at the failing target`)
    }
  }
})

test("frontend acceptance source guards reject drift and dirty entries on both jobs", () => {
  const directory = mkdtempSync(path.join(os.tmpdir(), "bamboo-frontend-source-"))
  const checkout = path.join(directory, "checkout")
  require("node:fs").mkdirSync(path.join(checkout, ".cargo"), { recursive: true })
  const git = (...args) => {
    const result = spawnSync("git", args, { cwd: checkout, encoding: "utf8" })
    assert.equal(result.status, 0, result.stderr)
    return result.stdout.trim()
  }
  try {
    git("init", "--quiet")
    writeFileSync(path.join(checkout, ".cargo/config.toml"), "# fixture\n")
    git("add", ".")
    git("-c", "user.name=ci-policy", "-c", "user.email=ci-policy@example.invalid", "commit", "--quiet", "-m", "fixture")
    const sha = git("rev-parse", "HEAD")
    const tree = git("rev-parse", "HEAD^{tree}")
    for (const source of [frontendWorkflow, frontendWorkflow.replace(/\n/gu, "\r\n")]) {
      for (const [index, frontendJob] of frontendJobs(source).entries()) {
        const script = frontendScript(frontendStep(frontendJob, "Require the exact clean candidate source"))
        const run = (env = {}) => spawnSync("bash", ["-e", "-o", "pipefail", "-c", script], {
          cwd: checkout, encoding: "utf8", env: { ...process.env,
            EXPECTED_SHA: sha,
            GITHUB_SHA: sha, GITHUB_EVENT_NAME: "pull_request", EXPECTED_SNAPSHOT_SHA: "",
            BUILD_SOURCE_SHA: sha, BUILD_SOURCE_TREE: tree,
            GITHUB_OUTPUT: path.join(directory, "outputs"),
            ...env,
          },
        })
        writeFileSync(path.join(directory, "outputs"), "")
        const accepted = run()
        assert.equal(accepted.status, 0, accepted.stderr)
        assert.equal(readFileSync(path.join(directory, "outputs"), "utf8"), `sha=${sha}\ntree=${tree}\n`)
        writeFileSync(path.join(directory, "outputs"), "")
        assert.equal(run({ GITHUB_EVENT_NAME: "workflow_dispatch", EXPECTED_SNAPSHOT_SHA: sha }).status, 0,
          "an exact manual snapshot is accepted")
        for (const snapshot of ["", "a".repeat(39), "A".repeat(40), "0".repeat(40)]) {
          assert.notEqual(run({ GITHUB_EVENT_NAME: "workflow_dispatch", EXPECTED_SNAPSHOT_SHA: snapshot }).status, 0,
            "missing, malformed or mismatched manual snapshots fail closed")
        }
        assert.notEqual(run({ GITHUB_EVENT_NAME: "workflow_dispatch", EXPECTED_SNAPSHOT_SHA: sha, GITHUB_SHA: "0".repeat(40) }).status, 0,
          "manual snapshot must bind the actual dispatch source")
        assert.equal(readFileSync(path.join(directory, "outputs"), "utf8"), `sha=${sha}\ntree=${tree}\n`)
        for (const env of [{ EXPECTED_SHA: "0".repeat(40) },
          ...(index === 1 ? [{ BUILD_SOURCE_SHA: "" }, { BUILD_SOURCE_TREE: "2".repeat(40) }] : [])]) {
          assert.notEqual(run(env).status, 0, "mismatched or missing identity fails closed")
        }
        writeFileSync(path.join(checkout, ".cargo/config.toml"), "# dirty\n")
        assert.notEqual(run().status, 0, "tracked changes fail closed")
        git("checkout", "--", ".cargo/config.toml")
        writeFileSync(path.join(checkout, "untracked.txt"), "fixture\n")
        assert.notEqual(run().status, 0, "untracked changes fail closed")
        rmSync(path.join(checkout, "untracked.txt"))
        const finalDiff = frontendStep(frontendJob, "Require source files to remain unchanged").split("        run: ")[1]
        assert.equal(spawnSync("bash", ["-e", "-c", finalDiff], { cwd: checkout }).status, 0)
        writeFileSync(path.join(checkout, ".cargo/config.toml"), "# test changed source\n")
        assert.notEqual(spawnSync("bash", ["-e", "-c", finalDiff], { cwd: checkout }).status, 0, "actual final diff rejects tested source changes")
        git("checkout", "--", ".cargo/config.toml")
      }
    }
  } finally {
    rmSync(directory, { recursive: true, force: true })
  }
})

test("frontend acceptance toolchain guards reject changed compilers and cross-job tools", () => {
  const directory = mkdtempSync(path.join(os.tmpdir(), "bamboo-frontend-toolchain-"))
  const rustcVersion = "rustc current-fixture"
  const cargoVersion = "cargo current-fixture"
  writeFileSync(path.join(directory, "rustc"), [
    "#!/bin/bash", 'printf "%s\\n" "$FIXTURE_RUSTC_VERSION"', "",
  ].join("\n"), { mode: 0o755 })
  writeFileSync(path.join(directory, "cargo"), '#!/bin/bash\nprintf "%s\\n" "$FIXTURE_CARGO_VERSION"\n', { mode: 0o755 })
  try {
    for (const [index, frontendJob] of frontendJobs(frontendWorkflow).entries()) {
      const script = frontendScript(frontendStep(frontendJob, "Record and bind the actual Rust toolchain"))
      const run = (env = {}) => spawnSync("bash", ["-e", "-o", "pipefail", "-c", script], {
        encoding: "utf8", env: { ...process.env,
          PATH: `${directory}${path.delimiter}${process.env.PATH}`,
          GITHUB_OUTPUT: path.join(directory, "outputs"),
          FIXTURE_RUSTC_VERSION: rustcVersion, FIXTURE_CARGO_VERSION: cargoVersion,
          BUILD_RUSTC_VERSION: rustcVersion, BUILD_CARGO_VERSION: cargoVersion, ...env,
        },
      })
      const accepted = run()
      assert.equal(accepted.status, 0, accepted.stderr)
      for (const env of [{ FIXTURE_RUSTC_VERSION: "" }, { FIXTURE_CARGO_VERSION: "" },
        ...(index === 1 ? [{ FIXTURE_RUSTC_VERSION: "rustc changed" }, { BUILD_RUSTC_VERSION: "" }, { BUILD_CARGO_VERSION: "" }, { FIXTURE_CARGO_VERSION: "cargo changed" }] : [])]) {
        assert.notEqual(run(env).status, 0, "changed or unbound toolchain fails closed")
      }
    }
  } finally {
    rmSync(directory, { recursive: true, force: true })
  }
})

test("frontend acceptance final guards inspect actual HEAD, index and untracked files", () => {
  const directory = mkdtempSync(path.join(os.tmpdir(), "bamboo-frontend-final-source-"))
  const statusTools = mkdtempSync(path.join(os.tmpdir(), "bamboo-frontend-status-failure-"))
  const realGit = spawnSync("bash", ["-c", "command -v git"], { encoding: "utf8" })
  assert.equal(realGit.status, 0, realGit.stderr)
  writeFileSync(path.join(statusTools, "git"), [
    "#!/bin/bash", 'if [[ "$1" == status ]]; then exit 23; fi', 'exec "$FIXTURE_REAL_GIT" "$@"', "",
  ].join("\n"), { mode: 0o755 })
  const git = (...args) => {
    const result = spawnSync("git", args, { cwd: directory, encoding: "utf8" })
    assert.equal(result.status, 0, result.stderr)
    return result.stdout.trim()
  }
  try {
    git("init", "--quiet")
    writeFileSync(path.join(directory, "source.txt"), "accepted\n")
    git("add", "source.txt")
    const commit = (...args) => git("-c", "user.name=ci-policy", "-c", "user.email=ci-policy@example.invalid", "commit", "--quiet", ...args)
    commit("-m", "accepted fixture")
    const sha = git("rev-parse", "HEAD")
    const tree = git("rev-parse", "HEAD^{tree}")
    for (const source of [frontendWorkflow, frontendWorkflow.replace(/\n/gu, "\r\n")]) {
      for (const frontendJob of frontendJobs(source)) {
        const finalScript = frontendScript(frontendStep(frontendJob, "Require the exact clean source after execution"))
        const run = (env = {}) => spawnSync("bash", ["-e", "-o", "pipefail", "-c", finalScript], {
          cwd: directory, encoding: "utf8",
          env: { ...process.env, EXPECTED_SHA: sha, EXPECTED_TREE: tree, ...env },
        })
        assert.equal(run().status, 0, "unchanged source is admitted")
        assert.equal(run({
          PATH: `${statusTools}${path.delimiter}${process.env.PATH}`,
          FIXTURE_REAL_GIT: realGit.stdout.trim(),
        }).status, 23, "git status failure must not be mistaken for a clean checkout")
        assert.notEqual(run({ EXPECTED_SHA: "0".repeat(40) }).status, 0)
        assert.notEqual(run({ EXPECTED_TREE: "1".repeat(40) }).status, 0)
        writeFileSync(path.join(directory, "source.txt"), "changed by test\n")
        assert.notEqual(run().status, 0, "unstaged source changes fail")
        git("add", "source.txt")
        git("diff", "--exit-code") // The original gate alone misses staged changes.
        assert.notEqual(run().status, 0, "staged source changes fail")
        assert.equal(git("diff", "--cached", "--name-only"), "source.txt", "guard must not erase staged evidence")
        git("restore", "--source=HEAD", "--staged", "--worktree", "source.txt")
        writeFileSync(path.join(directory, "unexpected.txt"), "test residue\n")
        git("diff", "--exit-code") // The original gate alone also misses untracked files.
        assert.notEqual(run().status, 0, "untracked files fail")
        assert.equal(readFileSync(path.join(directory, "unexpected.txt"), "utf8"), "test residue\n", "guard must not clean untracked evidence")
        git("add", "unexpected.txt")
        git("diff", "--exit-code")
        assert.notEqual(run().status, 0, "new staged files fail")
        git("rm", "--cached", "unexpected.txt")
        rmSync(path.join(directory, "unexpected.txt"))
        commit("--allow-empty", "-m", "unexpected HEAD with identical tree")
        assert.equal(git("rev-parse", "HEAD^{tree}"), tree)
        assert.notEqual(run().status, 0, "a different clean HEAD with the same tree fails")
        git("checkout", "--detach", sha)
        writeFileSync(path.join(directory, "source.txt"), "new clean source\n")
        git("add", "source.txt")
        commit("-m", "unexpected changed tree")
        assert.notEqual(run({ EXPECTED_SHA: git("rev-parse", "HEAD") }).status, 0, "a changed tree fails even if its HEAD is supplied")
        assert.equal(run({ EXPECTED_SHA: git("rev-parse", "HEAD"), EXPECTED_TREE: git("rev-parse", "HEAD^{tree}") }).status, 0,
          "a subsequent genuine event head and its recorded tree are accepted without a fixed source pin")
        git("checkout", "--detach", sha)
        assert.equal(run().status, 0, "restored accepted fixture is admitted")
      }
    }
  } finally {
    rmSync(directory, { recursive: true, force: true })
    rmSync(statusTools, { recursive: true, force: true })
  }
})

const actorWindowsWorkflow = readFileSync(".github/workflows/actor-snapshot-windows.yml", "utf8")
const actorWindowsController = readFileSync("scripts/actor-windows-native.ps1", "utf8")
const actorWindowsControllerTests = readFileSync("scripts/actor-windows-native.test.ps1", "utf8")
const actorWindowsStep = (source, name) => {
  const marker = `      - name: ${name}\n`
  const start = source.indexOf(marker)
  assert.notEqual(start, -1, `missing native Windows step: ${name}`)
  const end = source.indexOf("\n      - ", start + marker.length)
  return source.slice(start, end === -1 ? undefined : end)
}
const actorWindowsCompileName = "Compile and verify all 41 selected native tests"
const actorWindowsRunName = "Execute 9 snapshot, 3 gateway, 19 directory, 5 scoped and 5 initial process tests"

legacyTest("Supervisor driver self-tests cover dev, full validation and release platforms", (_, workflow, job, contributing) => {
  const name = "Test Supervisor driver and owned loopback process binding"
  const command = "node --test scripts/supervisor-native-acceptance.test.cjs"
  const step = (source) => {
    const block = actorWindowsStep(source, name)
    assert.equal(block.match(/^        run: (.+)$/mu)?.[1], command)
    assert.doesNotMatch(block, /continue-on-error:|\|\|\s*(?:true|exit 0)|--test-(?:name|skip)-pattern/u)
    return { block, condition: block.match(/^        if: (.+)$/mu)?.[1] ?? "true" }
  }
  const verify = (quick, full, platform) => {
    const fast = step(quick), comprehensive = step(full), native = step(platform)
    const quickIf = quick.match(/^    if: (.+)$/mu)?.[1]
    const platformIf = platform.match(/^    if: >-\n([\s\S]*?)(?=\n    [a-z])/mu)?.[1].trim().replace(/\s*\n\s*/gu, " ")
    assert.ok(quickIf && platformIf)
    const evaluate = (expression, github, full, os = "Linux") => runInNewContext(
      expression.replaceAll("needs.validation-source.outputs.full", "full"),
      { github, full, runner: { os }, startsWith: (value, prefix) => value.startsWith(prefix) },
    )
    for (const [event, base, head, fullValue, expected] of [
      ["pull_request", "dev", "feature/driver", "false", [true, false, false, false]],
      ["pull_request", "main", "release/" + "a".repeat(40), "true", [false, true, true, true]],
      ["workflow_dispatch", "", "", "true", [false, true, true, true]],
      ["push", "", "", "true", [false, true, false, false]],
    ]) {
      const github = { event_name: event, base_ref: base, head_ref: head, repository: "bigduu/Bamboo-agent",
        event: { pull_request: { head: { repo: { full_name: "bigduu/Bamboo-agent" } } } } }
      const matrix = evaluate(platformIf, github, fullValue)
      const actual = [
        evaluate(quickIf, github, fullValue) && evaluate(fast.condition, github, fullValue),
        evaluate(comprehensive.condition, github, fullValue),
        matrix && evaluate(native.condition, github, fullValue, "macOS"),
        matrix && evaluate(native.condition, github, fullValue, "Windows"),
      ]
      assert.deepEqual(actual, expected, event + ":" + base)
      assert.equal(evaluate(native.condition, github, fullValue, "Linux"), false,
        "the full Test job already runs the Linux suite")
    }
  }
  const quick = job("quick-checks"), full = job("test"), platform = job("build")
  verify(quick, full, platform)
  assert.throws(() => verify(quick, full.replace(name, "Removed driver coverage"), platform))
  const native = step(platform).block
  assert.throws(() => verify(quick, full, platform.replace(native,
    native.replace("runner.os != 'Linux'", "runner.os == 'Linux'"))))
  assert.throws(() => verify(quick, full.replace(command, "true"), platform))
})

test("feature portable Node suites have one owner per OS without duplicating Ubuntu", () => {
  const scripts = ["supervisor-native-acceptance.test.cjs", "lotus-next-artifact.test.cjs", "release-frontend-policy.test.cjs"]
  const counts = (source) => scripts.map((name) => [...source.matchAll(/run: node --test ([^\n]+)/gu)]
    .reduce((count, [, args]) => count + args.split(/\s+/u).filter((arg) => arg === `scripts/${name}`).length, 0))
  const verify = (quick, native, linux) => {
    assert.deepEqual(counts(quick), [1, 1, 1], "Ubuntu Quick owns each portable suite once")
    assert.deepEqual(counts(native), [1, 1, 1], "each native matrix entry owns each portable suite once")
    assert.deepEqual(counts(linux), [0, 0, 0], "the Linux Cargo executor does not repeat Quick Node suites")
    assert.match(native, /if: needs\.validation-source\.outputs\.feature == 'true'/u)
    assert.match(native, /os: \[windows-2025, macos-latest\]/u)
    const step = actorWindowsStep(native, "Test portable Supervisor and frontend policies")
    assert.doesNotMatch(step, /\bif:|continue-on-error:|\|\||--test-(?:name|skip)-pattern/u)
    assert.ok(native.indexOf(step) < native.indexOf("Run fixed native ownership"), "Node failures cannot be hidden after Cargo")
  }
  const quick = job("quick-checks"), native = job("native-feature-tests"), linux = job("feature-tests")
  verify(quick, native, linux)
  for (const script of scripts) {
    assert.throws(() => verify(quick.replace(` scripts/${script}`, ""), native, linux))
    assert.throws(() => verify(quick, native.replace(` scripts/${script}`, ""), linux))
  }
  assert.throws(() => verify(quick, native, linux + "\nrun: node --test scripts/lotus-next-artifact.test.cjs"))
  assert.throws(() => verify(quick, native.replace("os: [windows-2025, macos-latest]", "os: [macos-latest]"), linux))
  assert.throws(() => verify(quick, native.replace("- name: Test portable Supervisor and frontend policies", "- name: Test portable Supervisor and frontend policies\n        continue-on-error: true"), linux))
})

test("standalone native Windows is manual and feature CI owns automatic acceptance", () => {
  const header = actorWindowsWorkflow.split("\njobs:\n")[0]
  assert.match(header, /on:\n  workflow_dispatch:/u)
  assert.doesNotMatch(header, /pull_request:|push:/u)
  assert.match(job("native-feature-tests"), /run: node scripts\/native-test-owner\.cjs/u)
})

const assertActorWindowsAcceptance = (source, controller = actorWindowsController) => {
  const compileStep = actorWindowsStep(source, actorWindowsCompileName)
  const runtimeStep = actorWindowsStep(source, actorWindowsRunName)
  assert.match(compileStep, /-CompileAcceptance/u)
  assert.match(runtimeStep, /-ExecuteAcceptance/u)
  const compile = controller.split('function Compile-ActorNativeAcceptance(')[1].split('\nfunction ')[0]
  const runtime = controller.split('function Run-ActorNativeAcceptance(')[1].split('\nforeach ')[0]
  const groups = [...compile.matchAll(/Assert-TestList '([^']+)' '([^']+)' @\(([\s\S]*?)\n\s*\) '([^']+)'/gu)]
    .map(([, packageName, filter, body, log]) => ({
      package: packageName, filter,
      names: [...body.matchAll(/^\s*'([^']+)'\s*$/gmu)].map(([, name]) => name), log,
    }))
  // The exact original EF29 selection, including every name and its scope.
  assert.equal(createHash("sha256").update(JSON.stringify(groups)).digest("hex"),
    "b825cd60aa10b69ce33bcb0d92fff7e9c37878d3375f8008834b7e496c90fd52")
  assert.deepEqual(groups.map(({ names }) => names.length), [9, 3, 19, 5, 5])
  const runs = [...runtime.matchAll(/Assert-TestRun '([^']+)' '([^']+)' ([0-9]+) '([^']+)'/gu)]
    .map(([, packageName, filter, count]) => ({ package: packageName, filter, count: Number(count) }))
  assert.deepEqual(runs, groups.map(({ package: packageName, filter, names }) => ({
    package: packageName, filter, count: names.length,
  })))
  assert.match(compileStep, /timeout-minutes: 20\n/u)
  assert.match(runtimeStep, /timeout-minutes: 5\n/u)
  assert.doesNotMatch(runtime, /(?:&\s*|run:\s*)cargo\b/iu)
  assert.match(compile, /Save-ActorNativeManifest \$nativeContext/u)
  assert.match(runtime, /Read-ActorNativeManifest \$evidence/u)
  assert.match(runtime, /Assert-ActorNativeRun \$nativeContext \$package \$filter \$expectedCount \$logName/u)
}

test("native Windows retains the exact 41-case selection and original runtime budget", () => {
  assertActorWindowsAcceptance(actorWindowsWorkflow)
  assert.match(actorWindowsWorkflow, /runs-on: windows-2025\n    timeout-minutes: 30\n/u)
  assert.match(actorWindowsWorkflow, /contents: read/u)
  assert.match(actorWindowsWorkflow, /BAMBOO_FRONTEND_BUILD_MODE: api-only/u)
  assert.match(actorWindowsWorkflow, /RUST_MIN_STACK: '8388608'/u)
  assert.match(actorWindowsWorkflow, /toolchain: 1\.99\.0-x86_64-pc-windows-msvc/u)
  assert.match(actorWindowsController, /RUNNER_OS -ne 'Windows'.*RUNNER_ARCH -ne 'X64'/u)
  assert.match(actorWindowsController, /mklink \/J/u)
  assert.match(actorWindowsController, /\$junction\.LinkType -ne 'Junction'/u)
  assert.match(actorWindowsController, /fsutil reparsepoint query/u)
  assert.match(actorWindowsController, /'host: x86_64-pc-windows-msvc'/u)
  assert.match(actorWindowsController, /'release: 1\.99\.0'/u)
  assert.match(actorWindowsWorkflow, /Retain native Windows evidence even on failure\n        if: always\(\)/u)
  assert.throws(() => assertActorWindowsAcceptance(actorWindowsWorkflow, actorWindowsController.replace(
    "'background_scope_drain'", "'missing_original_case'")), "dropping or replacing a case must fail")
  assert.throws(() => assertActorWindowsAcceptance(actorWindowsWorkflow.replace(
    "timeout-minutes: 5\n", "timeout-minutes: 15\n")), "the runtime budget must not expand")
  assert.throws(() => assertActorWindowsAcceptance(actorWindowsWorkflow, actorWindowsController.replace(
    "$nativeContext = Read-ActorNativeManifest $evidence",
    "$nativeContext = Read-ActorNativeManifest $evidence\n          & cargo test --locked --no-fail-fast")),
  "runtime must not re-enter Cargo")
})

test("native Windows runs source-bound immutable artifacts and preserves process failures", () => {
  const functionBody = (name) => {
    const marker = `function ${name}(`
    const start = actorWindowsController.indexOf(marker)
    assert.notEqual(start, -1)
    const end = actorWindowsController.indexOf("\nfunction ", start + marker.length)
    return actorWindowsController.slice(start, end === -1 ? undefined : end)
  }
  const build = functionBody("Get-ActorNativeBinary")
  assert.match(build, /cargo test --locked -p \$Package --lib --no-run --message-format=json-render-diagnostics/u)
  assert.match(build, /\$Context\.packages\.ContainsKey\(\$Package\)/u)
  assert.match(build, /'build-finished'/u)
  assert.match(build, /\$units\.Count -ne 1/u)
  assert.match(build, /\[IO\.File\]::Copy\(\$executable, \$snapshot, \$false\)/u)
  for (const name of ["Invoke-ActorNativeProcess", "Assert-ActorNativeRun"]) {
    assert.doesNotMatch(functionBody(name), /&\s*cargo\b|cargo\s+test/iu)
  }
  const invoke = functionBody("Invoke-ActorNativeProcess")
  assert.ok(invoke.indexOf("Assert-ActorNativeArtifact") < invoke.indexOf("& $Invoker"))
  assert.match(invoke, /& \$Executable @NativeArguments/u)
  assert.match(invoke, /\$code = \$LASTEXITCODE/u)
  assert.match(invoke, /\[IO\.File\]::AppendAllText\(\$OutputPath,/u)
  assert.match(invoke, /\[IO\.FileMode\]::CreateNew/u)
  assert.doesNotMatch(invoke, /\$output = @\(& \$Executable/u)
  assert.ok(invoke.indexOf("AppendAllText($OutputPath") < invoke.indexOf("$code = $LASTEXITCODE"),
    "native output must be written while the executable runs")
  assert.ok(invoke.indexOf("AppendAllText($logPath") < invoke.indexOf("if ($result.ExitCode"),
    "injected failure output must be retained before rejecting its exit code")
  assert.match(invoke, /return @\(Get-Content -LiteralPath \$logPath\)/u)
  assert.match(invoke, /\$result\.ExitCode -isnot \[int\] -or \$result\.ExitCode -ne 0/u)
  assert.match(invoke, /Push-Location -LiteralPath \$Record\.workingDirectory/u)
  assert.match(invoke, /\$env:CARGO_MANIFEST_DIR = \$Record\.workingDirectory/u)
  assert.match(invoke, /finally \{\s*Pop-Location/u)
  const artifact = functionBody("Assert-ActorNativeArtifact")
  assert.match(artifact, /\$identity\.revision -cne \$Context\.revision/u)
  assert.match(artifact, /\$identity\.tree -cne \$Context\.tree/u)
  assert.match(artifact, /Get-FileHash -LiteralPath \$snapshot -Algorithm SHA256/u)
  assert.match(artifact, /FileAttributes\]::ReparsePoint/u)
  assert.match(functionBody("Get-ActorNativeIdentity"), /status --porcelain --untracked-files=all/u)
  assert.match(functionBody("Assert-ActorNativeRun"), /0 failed; 0 ignored/u)
  const fixture = actorWindowsStep(actorWindowsWorkflow, "Test native executable controller guards")
  assert.match(fixture, /run: \.\/scripts\/actor-windows-native\.test\.ps1/u)
  for (const failure of ["nonzero.*23", "counts did not prove", "no exit-code result",
    "selection differs", "Selected tests differ", "executable is missing, replaced or changed",
    "outside its owned directory", "source changed", "all five scopes and all 41 tests"]) {
    assert.ok(actorWindowsControllerTests.includes(`'${failure}'`), `missing controller failure fixture: ${failure}`)
  }
  for (const witness of ["Native failure output must be preserved before a nonzero exit is propagated",
    "partial output while still running", "Real native failure output lost stdout or stderr",
    "Timed-out native output must be retained before termination",
    "A timed-out controller was incorrectly reported as successful"]) {
    assert.ok(actorWindowsControllerTests.includes(witness), `missing output-retention regression: ${witness}`)
  }
})

test("actual source identity keeps unchanged Cargo fingerprints fresh", { timeout: 180000 }, (t) => {
  const { realpathSync } = require("node:fs")
  const directory = realpathSync(mkdtempSync(path.join(os.tmpdir(), "bamboo-source-watch-fixture-")))
  const evidence = realpathSync(mkdtempSync(path.join(process.env.RUNNER_TEMP || os.tmpdir(), "bamboo-source-watch-evidence-")))
  const modulePath = path.resolve("crates/infra/bamboo-skills/build/product_manual.rs")
  const moduleBytes = readFileSync(modulePath)
  const target = path.join(directory, "target")
  const repo = path.join(directory, "repo")
  const phases = [], commands = []
  let passed = false
  // Keep only compiler/runtime paths. Native children get neither host secrets
  // nor workflow write channels, user Git configuration, or Cargo overrides.
  const env = Object.fromEntries(Object.entries(process.env).filter(([key]) =>
    /^(?:PATH|HOME|USERPROFILE|TMP|TEMP|TMPDIR|SYSTEMROOT|WINDIR|COMSPEC|PATHEXT|CARGO_HOME|RUSTUP_HOME|RUSTUP_TOOLCHAIN)$/i.test(key)))
  Object.assign(env, { CARGO_TARGET_DIR: target, CARGO_INCREMENTAL: "0", CARGO_PROFILE_DEV_DEBUG: "0",
    CARGO_TERM_COLOR: "never", CARGO_HTTP_TIMEOUT: "20", CARGO_NET_RETRY: "0",
    CARGO_LOG: "cargo::core::compiler::fingerprint=info,cargo::compiler::fingerprint=info",
    GIT_CONFIG_NOSYSTEM: "1", GIT_CONFIG_GLOBAL: os.devNull })
  const lock = readFileSync("Cargo.lock", "utf8")
  const versions = [...lock.matchAll(/\[\[package\]\]\nname = "([^"]+)"\nversion = "([^"]+)"/g)]
  const dependencies = [["hex", "0.4."], ["serde_json", "1."], ["sha2", "0.11."]].map(([name, prefix]) => {
    const matches = versions.filter(row => row[1] === name && row[2].startsWith(prefix))
    assert.equal(matches.length, 1, "Use the existing module's locked build dependency: " + name)
    return `${name} = "=${matches[0][2]}"`
  }).join("\n")
  writeFileSync(path.join(evidence, "source-module.rs"), moduleBytes)
  function run(program, args, cwd, label) {
    const result = spawnSync(program, args, { cwd, env, encoding: "utf8", timeout: 60000, maxBuffer: 8 * 1024 * 1024 })
    commands.push({ program, args, cwd, label, status: result.status, error: result.error?.code })
    writeFileSync(path.join(evidence, label + ".stdout"), result.stdout || "")
    writeFileSync(path.join(evidence, label + ".stderr"), result.stderr || "")
    assert.equal(result.error, undefined, `${label}: ${result.error?.message}`)
    assert.equal(result.status, 0, `${label}: ${result.stderr}`)
    return (result.stdout || "").trim()
  }
  const git = (...args) => run("git", args, repo, "git-" + commands.length)
  function fixture(name, base = repo) {
    const manifest = path.join(base, "crates/infra", name)
    mkdirSync(path.join(manifest, "src"), { recursive: true })
    writeFileSync(path.join(manifest, "Cargo.toml"), `[package]\nname = "source-watch-${name}"\nversion = "0.0.0"\nedition = "2021"\n[workspace]\n[build-dependencies]\n${dependencies}\n`)
    writeFileSync(path.join(manifest, "src/lib.rs"), 'pub const COUNTER: &str = env!("SOURCE_WATCH_COUNTER");\npub const IDENTITY: &str = include_str!(concat!(env!("OUT_DIR"), "/identity.json"));\n')
    // Import the real module; source-module.rs above is evidence only. The
    // counter measures execution, unlike replayed build-script-executed JSON.
    writeFileSync(path.join(manifest, "build.rs"), `#[allow(dead_code)]\n#[path = ${JSON.stringify(modulePath)}]\nmod packaging;\nfn main() {\n  let root = std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());\n  let out = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap());\n  let counter = out.join("counter.txt");\n  let count = std::fs::read_to_string(&counter).ok().map(|v| v.parse::<u32>().unwrap()).unwrap_or(0) + 1;\n  std::fs::write(&counter, count.to_string()).unwrap();\n  let identity = packaging::source_identity(&root);\n  std::fs::write(out.join("identity.json"), serde_json::to_vec(&identity).unwrap()).unwrap();\n  println!("cargo:rustc-env=SOURCE_WATCH_COUNTER={count}");\n}\n`)
    return manifest
  }
  const key = unit => JSON.stringify([unit.package_id, unit.target, unit.features, unit.profile, unit.filenames])
  const previous = new Map()
  function build(manifest, label, count, identity, fresh) {
    const args = ["build", "--locked", "--message-format=json-render-diagnostics"]
    const stdout = run("cargo", args, manifest, label)
    const messages = stdout.split("\n").filter(Boolean).map(line => JSON.parse(line))
    const units = messages.filter(row => row.reason === "compiler-artifact")
    const product = units.filter(row => row.target.name === "source_watch_" + path.basename(manifest))
    assert.equal(product.length, 1)
    assert.ok(units.every(row => !row.target.name.startsWith("bamboo_")), "The fixture must not compile the Bamboo graph")
    const output = messages.filter(row => row.reason === "build-script-executed" && row.package_id === product[0].package_id)
    assert.equal(output.length, 1)
    const out = output[0].out_dir
    const actual = JSON.parse(readFileSync(path.join(out, "identity.json"), "utf8"))
    const runs = Number(readFileSync(path.join(out, "counter.txt"), "utf8"))
    const watches = [...readFileSync(path.join(out, "../output"), "utf8").matchAll(/^cargo:rerun-if-changed=(.+)$/gm)]
      .map(match => path.resolve(manifest, match[1]))
    const keys = units.map(key).sort()
    const record = { label, manifest, expectedRuns: count, runs, identity: actual,
      artifacts: units.length, fresh: units.filter(row => row.fresh).length, productFresh: product[0].fresh, watches }
    phases.push(record)
    writeFileSync(path.join(evidence, label + ".json"), JSON.stringify(record, null, 2) + "\n")
    writeFileSync(path.join(evidence, label + ".counter.txt"), readFileSync(path.join(out, "counter.txt")))
    writeFileSync(path.join(evidence, label + ".identity.json"), readFileSync(path.join(out, "identity.json")))
    assert.equal(runs, count, `${label}: build-script invocation count; evidence=${evidence}`)
    assert.deepEqual(actual, identity, label + ": exact source identity")
    assert.ok(watches.every(file => existsSync(file) && file !== path.join(repo, ".git") && !file.startsWith(target)),
      "Only existing bounded source-identity inputs may be watched")
    if (fresh) {
      assert.deepEqual(keys, previous.get(manifest), "An unchanged build must retain the exact Cargo units")
      assert.ok(units.every(row => row.fresh), label + ": every unchanged unit must be fresh")
    } else assert.equal(product[0].fresh, false, label + ": the changed identity must rebuild its product")
    previous.set(manifest, keys)
  }
  try {
    mkdirSync(path.join(repo, "builtin_skills"), { recursive: true })
    const ordinary = fixture("ordinary")
    git("init", "-q", "-b", "work")
    git("config", "user.name", "Source Watch Fixture")
    git("config", "user.email", "source-watch@example.invalid")
    git("config", "commit.gpgsign", "false")
    git("add", ".")
    git("commit", "-qm", "fixture source")
    git("checkout", "--detach", "-q")
    assert.equal(existsSync(path.join(ordinary, ".cargo_vcs_info.json")), false)
    assert.equal(existsSync(path.join(repo, ".git/packed-refs")), false)
    run("cargo", ["generate-lockfile"], ordinary, "resolve-fixture")
    let revision = git("rev-parse", "HEAD")
    const checkout = sha => ({ revision: sha, status: "checkout_unverified", identity_source: "git" })
    build(ordinary, "git-detached-first", 1, checkout(revision), false)
    build(ordinary, "git-detached-unchanged", 1, checkout(revision), true)
    writeFileSync(path.join(repo, "unrelated.txt"), "index refresh is not source cleanliness proof\n")
    git("add", "unrelated.txt")
    build(ordinary, "git-index-only", 1, checkout(revision), true)
    git("commit", "--allow-empty", "-qm", "detached identity changed")
    revision = git("rev-parse", "HEAD")
    build(ordinary, "git-detached-changed", 2, checkout(revision), false)
    build(ordinary, "git-detached-stable", 2, checkout(revision), true)
    git("checkout", "-q", "work")
    revision = git("rev-parse", "HEAD")
    build(ordinary, "git-symbolic-first", 3, checkout(revision), false)
    build(ordinary, "git-symbolic-unchanged", 3, checkout(revision), true)
    git("commit", "--allow-empty", "-qm", "loose reference changed")
    revision = git("rev-parse", "HEAD")
    build(ordinary, "git-symbolic-changed", 4, checkout(revision), false)
    build(ordinary, "git-symbolic-stable", 4, checkout(revision), true)
    git("pack-refs", "--all", "--prune")
    assert.equal(existsSync(path.join(repo, ".git/refs/heads/work")), false)
    build(ordinary, "git-packed-first", 5, checkout(revision), false)
    build(ordinary, "git-packed-unchanged", 5, checkout(revision), true)
    git("commit", "--allow-empty", "-qm", "packed reference becomes loose")
    revision = git("rev-parse", "HEAD")
    assert.equal(existsSync(path.join(repo, ".git/refs/heads/work")), true)
    build(ordinary, "git-packed-to-loose", 6, checkout(revision), false)
    build(ordinary, "git-packed-to-loose-stable", 6, checkout(revision), true)

    const archive = fixture("archive")
    run("cargo", ["generate-lockfile", "--offline"], archive, "resolve-archive")
    const vcs = path.join(archive, ".cargo_vcs_info.json")
    const archived = (sha, dirty) => ({ revision: sha, status: "source_archive", dirty, identity_source: "cargo_vcs_info" })
    writeFileSync(vcs, JSON.stringify({ git: { sha1: "a".repeat(40), dirty: true } }))
    build(archive, "archive-first", 1, archived("a".repeat(40), true), false)
    build(archive, "archive-unchanged", 1, archived("a".repeat(40), true), true)
    writeFileSync(vcs, JSON.stringify({ git: { sha1: "b".repeat(40), dirty: false } }))
    build(archive, "archive-changed", 2, archived("b".repeat(40), false), false)
    build(archive, "archive-stable", 2, archived("b".repeat(40), false), true)
    const unknown = { revision: null, status: "unknown", identity_source: "invalid_cargo_vcs_info" }
    for (const [index, value] of ["{malformed", "{}", JSON.stringify({ git: { sha1: "latest" } })].entries()) {
      writeFileSync(vcs, value)
      build(archive, "archive-invalid-" + index, 3 + index, unknown, false)
      build(archive, "archive-invalid-stable-" + index, 3 + index, unknown, true)
    }
    const noSource = fixture("unknown", path.join(directory, "unknown-source"))
    run("cargo", ["generate-lockfile", "--offline"], noSource, "resolve-unknown")
    const absent = { revision: null, status: "unknown", identity_source: "unknown" }
    build(noSource, "unknown-first", 1, absent, false)
    build(noSource, "unknown-unchanged", 1, absent, true)
    passed = true
  } finally {
    writeFileSync(path.join(evidence, "result.json"), JSON.stringify({ passed,
      modulePath, moduleSha256: createHash("sha256").update(moduleBytes).digest("hex"),
      fixtureLockSha256: existsSync(path.join(repo, "crates/infra/ordinary/Cargo.lock"))
        ? createHash("sha256").update(readFileSync(path.join(repo, "crates/infra/ordinary/Cargo.lock"))).digest("hex") : null,
      phases, commands }, null, 2) + "\n")
    rmSync(directory, { recursive: true, force: true })
    t.diagnostic("Source-watch module/counter/fingerprint evidence: " + evidence)
  }
})

test("Planner12/10 libraries have one canonical serial owner while API-only CLI and Clippy remain", () => {
  const { PLANNER_SERIAL, cargoArguments } = require("./run-feature-tests.cjs")
  assert.deepEqual(Object.keys(PLANNER_SERIAL), ["bamboo-server-tools", "bamboo-engine"])
  assert.deepEqual(Object.values(PLANNER_SERIAL).map(recipe => recipe.names.length), [12, 10])
  for (const [name, recipe] of Object.entries(PLANNER_SERIAL)) {
    assert.equal(new Set(recipe.names).size, recipe.names.length)
    assert.deepEqual(cargoArguments({ name }).slice(7), recipe.names.flatMap(name => ["--skip", name]))
  }
  const planner = readFileSync(".github/workflows/delegated-planner-acceptance.yml", "utf8")
  assert.equal([...planner.matchAll(/cargo test/g)].length, 1)
  assert.match(planner, /delegated_planner::real_cli_planner_/u)
  assert.match(planner, /test result: ok\. 2 passed; 0 failed; 0 ignored/u)
  assert.equal([...planner.matchAll(/cargo clippy/g)].length, 2)
  assert.match(planner, /BAMBOO_FRONTEND_BUILD_MODE: api-only/u)
})
