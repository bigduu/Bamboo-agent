const assert = require("node:assert/strict")
const { createHash } = require("node:crypto")
const { mkdtempSync, readFileSync, rmSync, writeFileSync } = require("node:fs")
const { spawnSync } = require("node:child_process")
const os = require("node:os")
const path = require("node:path")
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

test("macOS link guard preserves Cargo failures and rejects lost or oversized diagnostics", {
  skip: process.platform === "win32" && "the macOS helper requires a POSIX shell",
}, () => {
  const directory = mkdtempSync(path.join(os.tmpdir(), "bamboo-macos-link-guard-"))
  const helper = path.resolve(__dirname, "run-macos-server-lib-tests.sh")
  const expectedArgs = [
    "test", "--locked", "--profile", "dev", "-p", "bamboo-server",
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

test("full snapshot and main gates preserve locked Rust, formatting, and policy checks", () => {
  assert.match(workflow, /push:\n\s+branches: \[ main \]/u)
  assert.match(workflow, /pull_request:\n\s+branches: \[ dev, main \]/u)

  const testJob = job("test")
  assert.match(testJob, /name: Test\n/u)
  assert.match(testJob, /runs-on: ubuntu-latest\n/u)
  assert.ok(testJob.includes("    timeout-minutes: ${{ github.event_name == 'pull_request' && github.base_ref == 'dev' && 45 || 75 }}"))
  assert.match(testJob, /run: cargo build --locked\n/u)
  assert.match(
    testJob,
    /- name: Test locked Rust workspace\n        if: needs\.validation-source\.outputs\.full == 'true'\n        timeout-minutes: 45\n        run: cargo test --locked\n/u,
  )
  assert.match(
    testJob,
    /- name: Test CI workflow policy\n        if: needs\.validation-source\.outputs\.full == 'true'\n        run: node --test scripts\/ci-policy\.test\.cjs scripts\/crate-release\.test\.cjs scripts\/ci-routing\.test\.cjs\n/u,
  )
  assert.match(
    testJob,
    /- name: Check formatting\n        if: needs\.validation-source\.outputs\.full == 'true'\n        run: cargo fmt --all -- --check\n/u,
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
    /Pull requests into `dev` run the required\s+`Test` check in compile-only mode \(`cargo build --locked`\) and auxiliary\s+`Quick Checks`/u,
  )
  assert.match(contributing, /Before merge, owners must run affected tests locally and add or update coverage/u)
  assert.match(contributing, /for changed behavior, recording exact source, commands and results/u)
})

test("locked Rust test compilation has a separate budget before the complete suite", () => {
  const testJob = job("test")
  const [settings, ...steps] = testJob.split(/(?=^      - )/mu)
  assert.doesNotMatch(settings, /^    continue-on-error:/mu)
  assert.match(settings, /^    if: always\(\) && !cancelled\(\)$/mu)
  assert.ok(settings.includes("    timeout-minutes: ${{ github.event_name == 'pull_request' && github.base_ref == 'dev' && 45 || 75 }}"))

  const requiredSteps = [
    ["Build locked Rust workspace", "cargo build --locked", undefined],
    ["Compile locked Rust test targets", "cargo test --locked --no-run", "15"],
    ["Test locked Rust workspace", "cargo test --locked", "45"],
  ]
  let previousIndex = -1
  for (const [name, command, timeout] of requiredSteps) {
    const matches = steps.filter((step) => step.startsWith(`      - name: ${name}\n`))
    assert.equal(matches.length, 1, `${name} must appear exactly once`)
    const [step] = matches
    const index = steps.indexOf(step)
    assert.ok(index > previousIndex, `${name} must follow the preceding Rust step`)
    previousIndex = index

    assert.doesNotMatch(step, /^        continue-on-error:/mu)
    if (name === "Build locked Rust workspace") assert.doesNotMatch(step, /^        if:/mu)
    else assert.match(step, /^        if: needs\.validation-source\.outputs\.full == 'true'$/mu)
    assert.deepEqual(
      [...step.matchAll(/^        run: (.*)$/gmu)].map((match) => match[1]),
      [command],
      `${name} must retain the exact locked command`,
    )
    assert.deepEqual(
      [...step.matchAll(/^        timeout-minutes: (.*)$/gmu)].map((match) => match[1]),
      timeout === undefined ? [] : [timeout],
      `${name} must retain its time budget`,
    )
    if (timeout !== undefined) {
      assert.match(step, /^        env:\n          RUST_MIN_STACK: "8388608"$/mu)
    }
    assert.equal(
      steps.filter((candidate) => candidate.split("\n").includes(`        run: ${command}`))
        .length,
      1,
      `${command} must not run again under another step name`,
    )
  }
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
  const sourceStep = build.indexOf("- name: Verify portable Skill source publication")
  const selectedStep = build.indexOf("- name: Verify portable selected Skill source")
  const readToolStep = build.indexOf("- name: Verify portable Skill read Tool")
  const releaseStep = build.indexOf("- name: Build\n")
  assert.ok(sourceStep >= 0 && selectedStep > sourceStep && releaseStep > selectedStep)
  assert.match(build, /run: cargo test --locked -p bamboo-skills progressive::read_tests -- --test-threads=1\n/u)
  assert.equal((workflow.match(/progressive::read_tests/gu) || []).length, 1)
  assert.ok(readToolStep > selectedStep && readToolStep < releaseStep)
  assert.match(build, /run: cargo test --locked -p bamboo-server-tools skill_runtime::catalog_tests -- --test-threads=1\n/u)
  assert.equal((workflow.match(/skill_runtime::catalog_tests/gu) || []).length, 1)
  assert.doesNotMatch(job("test"), /Verify portable Skill read Tool/u)

  assert.match(
    build,
    /- name: Test frontend artifact and release policies\n        if: runner\.os != 'Linux'\n/u,
  )
  assert.match(
    build,
    /- name: Verify portable frontend package contract\n        if: runner\.os != 'Linux'\n/u,
  )
})

test("all-feature E2E tests retain the workspace test thread stack contract", () => {
  const steps = job("e2e-test").split(/(?=^      - )/mu)
  const matches = steps.filter((step) =>
    step.startsWith("      - name: Run all-feature library and integration tests\n"),
  )
  assert.equal(matches.length, 1, "the all-feature E2E step must appear exactly once")
  const [step] = matches
  assert.deepEqual(
    [...step.matchAll(/^        run: (.*)$/gmu)].map((match) => match[1]),
    ["cargo test --locked --all-features --lib --tests"],
  )
  assert.match(step, /^        env:\n          RUST_MIN_STACK: "8388608"$/mu)
  assert.doesNotMatch(step, /^        (?:if|continue-on-error):/mu)
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
    assert.ok(job(id).includes("if: needs.validation-source.outputs.full == 'true'"), `${id} must be comprehensive-only`)
  }
})

test("main promotion and manual build policy stays intact", () => {
  const promotion = job("promotion-source")
  assert.match(promotion, /github\.base_ref == 'main'/u)
  assert.match(promotion, /HEAD_REF.*github\.head_ref/u)

  const build = job("build")
  assert.match(build, /github\.event_name == 'workflow_dispatch'/u)
  assert.match(build, /github\.base_ref == 'main'/u)
  assert.match(build, /startsWith\(github\.head_ref, 'release\/'\)/u)
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

const promotionScript = () => {
  const block = job("promotion-source").match(/        run: \|\n([\s\S]*)$/u)
  assert.ok(block, "source proof must stay inline before any checkout")
  return block[1].split("\n").map((line) => line.replace(/^          /u, "")).join("\n")
}

test("frozen promotion bootstrap is read-only and leaves merge-tree validation intact", () => {
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
  for (const id of ["test", "lint", "e2e-test", "build"]) {
    assert.match(job(id), /- uses: actions\/checkout@v7\n\n/u)
  }
  for (const context of ["Promotion Source", "Test", "Lint", "E2E Tests", "Build (${{ matrix.os }})"]) {
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
  assert.match(contributing, /full separate `F` release-source and `U` protected-dev integration procedure/u)
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
const frontendHead = "${{ github.event_name == 'pull_request' && github.event.pull_request.head.sha || github.sha }}"
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
  const header = source.slice(0, source.indexOf("\njobs:\n")).split("\n").filter((line) => line.trim() && !line.startsWith("#"))
  assert.deepEqual(header, [
    "name: Frontend consumer acceptance",
    "on:",
    "  workflow_dispatch:",
    "    inputs:",
    "      snapshot_sha:",
    "        description: Exact 40-character SHA of the selected ref for source acceptance",
    "        required: true",
    "        type: string",
    "  pull_request:",
    "    branches: [main]",
    "permissions:",
    "  contents: read",
    "env:",
    "  PYTHONDONTWRITEBYTECODE: \"1\"",
  ], "main and manual snapshot routing binds the actual source without Cargo environment overrides")
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
    "          save-if: ${{ github.event_name == 'workflow_dispatch' || (github.event_name == 'pull_request' && github.base_ref == 'dev') }}",
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
  ].join("\n"), "the current lint job, snapshot guard, Clippy arguments and cache policy remain unchanged")
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
      ...(index === 0 ? ["Build locked Rust workspace"] : ["Compile locked Rust test targets", "Test locked Rust workspace"]),
      "Require source files to remain unchanged",
      "Require the exact clean source after execution",
    ])
    assert.equal(frontendStep(frontendJob, "Setup Node.js"), "      - name: Setup Node.js\n        uses: actions/setup-node@v7\n        with:\n          node-version: lts/*\n          package-manager-cache: false")
    assert.equal(frontendStep(frontendJob, "Install Rust"), "      - name: Install Rust\n        uses: dtolnay/rust-toolchain@stable\n        with:\n          components: rustfmt")
    for (const [name, command, timeout] of [
      ["Test original CI policy", "node --test scripts/ci-policy.test.cjs"],
      ["Check formatting", "cargo fmt --all -- --check"],
      ...(index === 0 ? [["Build locked Rust workspace", "cargo build --locked"]] : [
        ["Compile locked Rust test targets", "cargo test --locked --no-run", 15],
        ["Test locked Rust workspace", "cargo test --locked", 45],
      ]),
      ["Require source files to remain unchanged", "git diff --exit-code"],
    ]) {
      assert.equal(frontendStep(frontendJob, name), `      - name: ${name}\n${timeout ? `        timeout-minutes: ${timeout}\n` : ""}        run: ${command}${timeout ? '\n        env:\n          RUST_MIN_STACK: "8388608"' : ""}`)
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

test("frontend acceptance preserves both original command gates and actual-head identity", () => {
  assert.equal(require("node:fs").existsSync(".github/workflows/frontend-acceptance-recovery.yml"), false,
    "the unused fixed-source recovery controller must not coexist with the generic workflow")
  assertFrontendPolicy(frontendWorkflow)
  for (const [from, to] of [
    ["  workflow_dispatch:", "  workflow_dispatch:\n    inputs:\n      source:\n        type: string"],
    ["    branches: [main]", "    branches: [dev]"],
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
    ["name: Test locked Rust workspace\n        timeout-minutes: 45", "name: Test locked Rust workspace\n        timeout-minutes: 30"],
    ["name: Test locked Rust workspace\n        timeout-minutes: 45", "name: Test locked Rust workspace\n        timeout-minutes: 60"],
    ["package-manager-cache: false", "package-manager-cache: true"],
    ["components: rustfmt\n", "toolchain: 1.99.0\n          components: rustfmt\n"],
    ["run: cargo build --locked", "run: true"],
    ["run: cargo test --locked\n", "run: cargo test --locked -- --skip slow\n"],
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
    ["        run: cargo test --locked --no-run", "        continue-on-error: true\n        run: cargo test --locked --no-run"],
    ["    needs: build", "    if: always()\n    needs: build"],
    ["      - name: Check formatting", "      - uses: actions/cache@v4\n      - name: Check formatting"],
    ["      - name: Require source files to remain unchanged", "      - name: Reset source\n        run: git reset --hard\n      - name: Require source files to remain unchanged"],
  ]) {
    assert.ok(frontendWorkflow.includes(from), `mutation target exists: ${from}`)
    assert.throws(() => assertFrontendPolicy(frontendWorkflow.replace(from, to)), `reject unsafe frontend mutation: ${to}`)
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

const assertActorWindowsAcceptance = (source) => {
  const compile = actorWindowsStep(source, actorWindowsCompileName)
  const runtime = actorWindowsStep(source, actorWindowsRunName)
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
  assert.match(compile, /timeout-minutes: 20\n/u)
  assert.match(runtime, /timeout-minutes: 5\n/u)
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
  assert.match(actorWindowsWorkflow, /RUNNER_OS -ne 'Windows'.*RUNNER_ARCH -ne 'X64'/u)
  assert.match(actorWindowsWorkflow, /mklink \/J/u)
  assert.match(actorWindowsWorkflow, /\$junction\.LinkType -ne 'Junction'/u)
  assert.match(actorWindowsWorkflow, /fsutil reparsepoint query/u)
  assert.match(actorWindowsWorkflow, /'host: x86_64-pc-windows-msvc'/u)
  assert.match(actorWindowsWorkflow, /'release: 1\.99\.0'/u)
  assert.match(actorWindowsWorkflow, /Retain native Windows evidence even on failure\n        if: always\(\)/u)
  assert.throws(() => assertActorWindowsAcceptance(actorWindowsWorkflow.replace(
    "'background_scope_drain'", "'missing_original_case'")), "dropping or replacing a case must fail")
  assert.throws(() => assertActorWindowsAcceptance(actorWindowsWorkflow.replace(
    "timeout-minutes: 5\n", "timeout-minutes: 15\n")), "the runtime budget must not expand")
  assert.throws(() => assertActorWindowsAcceptance(actorWindowsWorkflow.replace(
    "$nativeContext = Read-ActorNativeManifest $evidence",
    "$nativeContext = Read-ActorNativeManifest $evidence\n          & cargo test --locked")),
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
