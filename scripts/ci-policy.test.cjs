const assert = require("node:assert/strict")
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
    /- name: Test locked Rust workspace\n        if: needs\.validation-source\.outputs\.full == 'true'\n        timeout-minutes: 30\n        run: cargo test --locked\n/u,
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
    ["Test locked Rust workspace", "cargo test --locked", "30"],
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
