const assert = require("node:assert/strict")
const { readFileSync } = require("node:fs")
const { test } = require("node:test")

const workflow = readFileSync(".github/workflows/ci.yml", "utf8")
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

test("routine dev pull requests run one locked Rust build-and-test gate", () => {
  assert.match(workflow, /push:\n\s+branches: \[ main \]/u)
  assert.match(workflow, /pull_request:\n\s+branches: \[ dev, main \]/u)

  const testJob = job("test")
  assert.match(testJob, /run: cargo build --locked\n/u)
  assert.match(testJob, /run: cargo test --locked\n/u)

  for (const name of [
    "Setup Node.js",
    "Test frontend artifact and release policies",
    "Stage frontend package",
    "Verify published server crate owns the frontend package",
    "Run all-feature Rust tests",
    "Build examples",
    "Run real SSH/SFTP transport test",
  ]) {
    assert.ok(
      testJob.includes(`name: ${name}\n        ${comprehensiveOnly}`),
      `${name} must be comprehensive-only`,
    )
  }

  assert.match(
    testJob,
    /run: cargo test --locked --all-features --lib --tests\n/u,
  )
  assert.match(testJob, /run: cargo build --locked --examples\n/u)

  assert.match(
    contributing,
    /Pull requests into `dev` run only `cargo build --locked` and `cargo test --locked` in the required `Test` gate\./u,
  )
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
