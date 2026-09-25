const assert = require("node:assert/strict")
const { readFileSync } = require("node:fs")
const { test } = require("node:test")

const workflow = readFileSync(".github/workflows/ci.yml", "utf8")
const cacheCleanupWorkflow = readFileSync(
  ".github/workflows/pr-cache-cleanup.yml",
  "utf8",
)
const codeqlWorkflow = readFileSync(".github/workflows/codeql.yml", "utf8")
const contributing = readFileSync("CONTRIBUTING.md", "utf8")

const job = (id) => {
  const marker = `\n  ${id}:\n`
  const start = workflow.indexOf(marker)
  assert.notEqual(start, -1, `missing CI job: ${id}`)

  const contentStart = start + marker.length
  const nextJob = workflow.slice(contentStart).search(/\n  [a-z][a-z0-9-]*:\n/u)
  return nextJob === -1
    ? workflow.slice(contentStart)
    : workflow.slice(contentStart, contentStart + nextJob)
}

const comprehensiveOnly =
  "if: github.event_name != 'pull_request' || github.base_ref == 'main'"

test("routine dev pull requests run locked Rust, formatting, and policy checks", () => {
  assert.match(workflow, /push:\n\s+branches: \[ main \]/u)
  assert.match(workflow, /pull_request:\n\s+branches: \[ dev, main \]/u)

  const testJob = job("test")
  assert.match(testJob, /name: Test\n/u)
  assert.match(testJob, /run: cargo build --locked\n/u)
  assert.match(testJob, /run: cargo test --locked\n/u)
  assert.match(
    testJob,
    /- name: Test CI workflow policy\n        run: node --test scripts\/ci-policy\.test\.cjs\n/u,
  )
  assert.match(
    testJob,
    /- name: Check formatting\n        run: cargo fmt --all -- --check\n/u,
  )
  assert.match(
    testJob,
    /- name: Setup Node\.js\n        uses: actions\/setup-node@v7\n        with:\n          node-version: lts\/\*/u,
    "Node must be installed before Rust hook tests on dev pull requests",
  )

  for (const name of [
    "Test frontend artifact and release policies",
    "Stage frontend package",
    "Verify published server crate owns the frontend package",
    "Build examples",
    "Run real SSH/SFTP transport test",
  ]) {
    assert.ok(
      testJob.includes(`name: ${name}\n        ${comprehensiveOnly}`),
      `${name} must be comprehensive-only`,
    )
  }

  assert.doesNotMatch(testJob, /run: cargo test --locked --all-features/u)
  assert.match(testJob, /run: cargo build --locked --examples\n/u)

  assert.match(
    contributing,
    /Pull requests into `dev` run locked Rust build\/test, formatting, and CI workflow policy checks in the required `Test` gate\./u,
  )
})

test("promotion retains required checks without repeating platform coverage", () => {
  assert.match(job("promotion-source"), /name: Promotion Source\n/u)
  assert.match(job("lint"), /name: Lint\n/u)

  const e2e = job("e2e-test")
  assert.match(e2e, /name: E2E Tests\n/u)
  assert.match(e2e, /run: cargo test --locked --all-features --lib --tests\n/u)
  assert.equal(
    [...workflow.matchAll(/run: cargo test --locked --all-features --lib --tests\n/gu)]
      .length,
    1,
  )
  assert.doesNotMatch(workflow, /cargo test --test e2e_tests --all-features/u)

  const tls = job("tls-fixture-test")
  assert.match(tls, /os: \[macos-latest, windows-latest\]/u)
  assert.match(tls, /if: runner\.os == 'macOS'\n        run: scripts\/run-macos-server-lib-tests\.sh/u)
  assert.match(tls, /if: runner\.os == 'Windows'\n        run: cargo test --locked -p bamboo-server --lib server::tls::tests/u)

  const build = job("build")
  assert.match(build, /name: Build \(\$\{\{ matrix\.os \}\}\)\n/u)
  assert.match(build, /os: \[ubuntu-latest, macos-latest, windows-latest\]/u)
  assert.match(build, /run: cargo build --release --verbose\n/u)
  assert.match(
    build,
    /- name: Test frontend artifact and release policies\n        if: runner\.os != 'Linux'\n/u,
  )
  assert.match(
    build,
    /- name: Verify portable frontend package contract\n        if: runner\.os != 'Linux'\n/u,
  )
})

test("PR caches are reusable while open and scoped cleanup runs on close", () => {
  assert.match(
    job("test"),
    /save-if: \$\{\{ github\.event_name == 'push' \|\| \(github\.event_name == 'pull_request' && github\.base_ref == 'dev'\) \}\}/u,
  )
  for (const id of [
    "msrv",
    "tls-fixture-test",
    "tool-event-recorder-e2e",
    "e2e-test",
    "lint",
    "docs",
    "security",
  ]) {
    assert.match(
      job(id),
      /save-if: \$\{\{ github\.event_name == 'push' && github\.ref == 'refs\/heads\/main' \}\}/u,
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

test("expensive validation jobs stay off dev pull requests", () => {
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
    assert.ok(job(id).includes(comprehensiveOnly), `${id} must be comprehensive-only`)
  }
})

test("main promotion and manual build policy stays intact", () => {
  const promotion = job("promotion-source")
  assert.match(promotion, /github\.base_ref == 'main'/u)
  assert.match(promotion, /HEAD_REF.*dev/u)

  const build = job("build")
  assert.match(build, /github\.event_name == 'workflow_dispatch'/u)
  assert.match(build, /github\.base_ref == 'main'/u)
  assert.match(build, /github\.head_ref == 'dev'/u)
  assert.match(build, /os: \[ubuntu-latest, macos-latest, windows-latest\]/u)
})

test("CodeQL stays on the main and manual paths", () => {
  assert.match(codeqlWorkflow, /workflow_dispatch:\n/u)
  assert.match(codeqlWorkflow, /push:\n\s+branches: \[ main \]/u)
  assert.match(codeqlWorkflow, /pull_request:\n\s+branches: \[ main \]/u)
  assert.doesNotMatch(codeqlWorkflow, /branches: \[[^\]]*dev/u)
  assert.match(
    codeqlWorkflow,
    /language: \[ actions, javascript-typescript, python, rust \]/u,
  )
  assert.match(codeqlWorkflow, /build-mode: none/u)
})
