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

test("routine dev pull requests run locked Rust, formatting, and policy checks", () => {
  assert.match(workflow, /push:\n\s+branches: \[ main \]/u)
  assert.match(workflow, /pull_request:\n\s+branches: \[ dev, main \]/u)

  const testJob = job("test")
  assert.match(testJob, /name: Test\n/u)
  assert.match(testJob, /name: Test\n    runs-on: ubuntu-latest\n    timeout-minutes: 75\n/u)
  assert.match(testJob, /run: cargo build --locked\n/u)
  assert.match(
    testJob,
    /- name: Test locked Rust workspace\n        timeout-minutes: 30\n        run: cargo test --locked\n/u,
  )
  assert.match(
    testJob,
    /- name: Test CI workflow policy\n        run: node --test scripts\/ci-policy\.test\.cjs scripts\/crate-release\.test\.cjs\n/u,
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

test("locked Rust test compilation has a separate budget before the complete suite", () => {
  const testJob = job("test")
  const [settings, ...steps] = testJob.split(/(?=^      - )/mu)
  assert.doesNotMatch(settings, /^    (?:if|continue-on-error):/mu)
  assert.match(settings, /^    timeout-minutes: 75$/mu)

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

    assert.doesNotMatch(step, /^        (?:if|continue-on-error):/mu)
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
    assert.ok(job(id).includes(comprehensiveOnly), `${id} must be comprehensive-only`)
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
  assert.match(contributing, /current `dev` tip must be an ancestor\n   of the fix head/u)
  assert.match(contributing, /stop this cut's fix path/u)
  assert.match(contributing, /Do not fast-forward that updated dev-based fix into the old release branch/u)
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
    const feature = commit("unrelated ongoing development")
    git("branch", `release/${cut}`, cut)
    git("switch", "--quiet", "-c", "required-release-fix", cut)
    const fixFile = "bounded-release-fix.txt"
    const fix = commit("bounded release fix", fixFile)
    const pending = commit("not yet backmerged")
    git("switch", "--quiet", "dev")
    const isAncestor = (source, target) => {
      const result = spawnSync("git", ["merge-base", "--is-ancestor", source, target], {
        cwd: directory, encoding: "utf8",
      })
      assert.ok([0, 1].includes(result.status), result.stderr)
      return result.status === 0
    }
    assert.equal(isAncestor(feature, fix), false, "a release-only fix lacks the current dev base")
    // This local FF refusal models only strict base ancestry; GitHub need not
    // use FF merges, and this fixture does not attempt a protected GitHub PR.
    const staleFix = spawnSync("git", ["merge", "--ff-only", fix], {
      cwd: directory, encoding: "utf8",
    })
    assert.notEqual(staleFix.status, 0)
    assert.equal(git("rev-parse", "dev"), feature)
    assert.equal(git("rev-parse", `release/${cut}`), cut, "stop the old cut's necessary-fix path")

    git("switch", "--quiet", "-c", "required-fix-on-current-dev", "dev")
    const originalFix = spawnSync("git", ["show", `${fix}:${fixFile}`], { cwd: directory })
    assert.equal(originalFix.status, 0, originalFix.stderr.toString())
    const fixBytes = originalFix.stdout
    writeFileSync(path.join(directory, fixFile), fixBytes)
    git("add", fixFile)
    git("commit", "--quiet", "-m", "necessary fix based on current dev")
    const updatedFix = git("rev-parse", "HEAD")
    assert.equal(isAncestor(feature, updatedFix), true, "the replacement PR contains actual current dev")
    assert.equal(isAncestor(fix, updatedFix), false, "the old fix is not presented as normally accepted")
    assert.deepEqual(readFileSync(path.join(directory, fixFile)), fixBytes)
    assert.equal(git("diff", "--binary", cut, fix, "--", fixFile),
      git("diff", "--binary", feature, updatedFix, "--", fixFile), "carry only the exact necessary fix patch")
    git("switch", "--quiet", "dev")
    // Advance the toy accepted-dev ref only after proving the strict premise.
    git("merge", "--quiet", "--ff-only", updatedFix)
    const reviewed = git("rev-parse", "HEAD")
    const newCut = git("rev-parse", "dev")
    assert.equal(newCut, updatedFix, "freshly read the actual accepted dev tip")
    git("branch", `release/${newCut}`, newCut)
    assert.equal(git("rev-parse", `release/${newCut}`), reviewed)
    assert.equal(git("rev-parse", `release/${cut}`), cut, "never fast-forward updated dev into the old release")
    assert.equal(isAncestor(feature, cut), false, "unrelated dev work stays outside the old release")
    const advanced = commit("later dev work")

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
        release: [reviewed, reviewed], dev: [reviewed, reviewed], ...fixture,
      }))
      return spawnSync("python3", ["-c", fixtureProgram], {
        cwd: directory, encoding: "utf8", env: { ...process.env,
          GITHUB_REPOSITORY: "bigduu/Bamboo-agent", GH_TOKEN: "fixture-read-only",
          HEAD_REPOSITORY: "bigduu/Bamboo-agent", HEAD_REF: `release/${newCut}`, HEAD_SHA: reviewed,
          ...env,
        },
      })
    }
    for (const [name, fixture] of [
      ["new cut freshly read from normally accepted current dev", {}],
      ["original no-fix cut remains accepted after dev advances", { release: [cut, cut], env: { HEAD_REF: `release/${cut}`, HEAD_SHA: cut } }],
      ["later dev tip", { dev: [advanced, advanced] }],
      ["API origin cannot be changed by environment", { env: { GITHUB_API_URL: "https://invalid.example" } }],
      ["dev advances between metadata reads", { dev: [reviewed, advanced], counts: { release: 2, dev: 2, compare: 3 } }],
      ["dev and new release head may initially be identical", { dev: [reviewed, reviewed] }],
    ]) {
      const result = run(fixture)
      assert.equal(result.status, 0, `${name}: ${result.stderr}`)
      assert.match(result.stdout, /Accepted frozen release/u)
    }
    for (const [name, fixture] of [
      ["fork", { env: { HEAD_REPOSITORY: "someone/Bamboo-agent" } }],
      ["unexpected target repository", { env: { GITHUB_REPOSITORY: "someone/Bamboo-agent", HEAD_REPOSITORY: "someone/Bamboo-agent" } }],
      ["direct dev", { env: { HEAD_REF: "dev" } }],
      ["malformed cut", { env: { HEAD_REF: "release/latest" } }],
      ["short cut", { env: { HEAD_REF: `release/${cut.slice(0, 12)}` } }],
      ["uppercase cut", { env: { HEAD_REF: `release/${cut.toUpperCase()}` } }],
      ["wrong cut ancestor", { env: { HEAD_REF: `release/${fix}` } }],
      ["missing head", { env: { HEAD_SHA: "" } }],
      ["missing token", { env: { GH_TOKEN: "" } }],
      ["release head mismatches PR event", { release: [pending, pending] }],
      ["release moves during proof", { release: [reviewed, pending] }],
      ["old release fix lacks normal dev acceptance", { release: [fix, fix], env: { HEAD_REF: `release/${cut}`, HEAD_SHA: fix } }],
      ["unbackmerged release commit", { release: [pending, pending], env: { HEAD_REF: `release/${cut}`, HEAD_SHA: pending } }],
      ["dev rewrites to history that excludes the release fix", { dev: [reviewed, feature], counts: { release: 2, dev: 2, compare: 3 } }],
      ["dev rewrites even though the release head remains an ancestor", { dev: [advanced, reviewed], counts: { release: 2, dev: 2, compare: 3 } }],
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

// This controller tests fixed release source C, not its own head R. Its policy
// lives here so normal required PR CI validates R without dispatching recovery.
const recoveryWorkflow = readFileSync(
  ".github/workflows/frontend-acceptance-recovery.yml", "utf8",
)
const recoverySha = "1cda5e1dbae79769052300b26d134bd4ecccf9b3"
const recoveryTree = "1af37ea4e8b13a5b026b283a535c38f6dd84690c"
const recoveryJobs = (source) => {
  const parts = source.replace(/\r\n/gu, "\n").split("\njobs:\n")
  assert.equal(parts.length, 2, "exactly one jobs block is required")
  const sections = parts[1].split(/^  ([a-z][a-z-]*):\n/gmu)
  assert.equal(sections.length, 5, "exactly the build and test jobs are allowed")
  assert.deepEqual([sections[1], sections[3]], ["build", "test"])
  return [sections[2], sections[4]].map((section) => {
    const [settings, ...steps] = section.split(/(?=^      - )/mu)
    return { settings, steps: steps.map((step) => step.trimEnd()) }
  })
}
const recoveryStep = (job, name) => {
  const matches = job.steps.filter((step) => step.startsWith(`      - name: ${name}\n`))
  assert.equal(matches.length, 1, `exactly one ${name} step is required`)
  return matches[0]
}
const recoveryScript = (step) => {
  const match = step.match(/^        run: \|\n([\s\S]*)$/mu)
  assert.ok(match, "guard must be an executable block")
  return match[1].split("\n").map((line) => line.replace(/^          /u, "")).join("\n")
}
function assertRecoveryPolicy(source) {
  source = source.replace(/\r\n/gu, "\n")
  const header = source.slice(0, source.indexOf("\njobs:\n")).split("\n").filter((line) => line.trim() && !line.startsWith("#"))
  assert.deepEqual(header, [
    "name: Frozen frontend Test recovery", "on:", "  workflow_dispatch:", "permissions:", "  contents: read", "env:",
    `  EXPECTED_SOURCE_SHA: ${recoverySha}`, `  EXPECTED_SOURCE_TREE: ${recoveryTree}`,
  ], "the manual controller has no other triggers, inputs or environment overrides")
  assert.match(source, /\non:\n  workflow_dispatch:\n\npermissions:\n  contents: read\n\nenv:\n/u)
  assert.match(source, new RegExp(`^  EXPECTED_SOURCE_SHA: ${recoverySha}$`, "mu"))
  assert.match(source, new RegExp(`^  EXPECTED_SOURCE_TREE: ${recoveryTree}$`, "mu"))
  assert.doesNotMatch(source, /^\s*(?:if|continue-on-error|working-directory|defaults|strategy|container|services|secrets|environment):/mu)
  assert.doesNotMatch(source, /(?:actions\/(?:cache|upload-artifact|download-artifact)|rust-cache)@|\b(?:git reset|git clean|git checkout|git switch|cargo install|cargo publish)\b/u)
  assert.equal((source.match(/^permissions:/gmu) || []).length, 1)
  assert.equal((source.match(/^[ \t]+permissions:/gmu) || []).length, 0)
  assert.deepEqual([...source.matchAll(/^      - uses: (.*)$/gmu)].map((m) => m[1]), ["actions/checkout@v7", "actions/checkout@v7"])
  assert.deepEqual([...source.matchAll(/^        uses: (.*)$/gmu)].map((m) => m[1]), ["actions/setup-node@v7", "dtolnay/rust-toolchain@stable", "actions/setup-node@v7", "dtolnay/rust-toolchain@stable"])
  const jobs = recoveryJobs(source)
  for (const [index, recoveryJob] of jobs.entries()) {
    assert.doesNotMatch(recoveryJob.settings, /^    env:/mu, "job environments must not alter Cargo or test behavior")
    assert.match(recoveryJob.settings, /^    runs-on: ubuntu-latest\n    timeout-minutes: 45$/mu)
    if (index === 0) {
      assert.doesNotMatch(recoveryJob.settings, /^    needs:/mu)
      for (const [name, step, field] of [["source_sha", "source", "sha"], ["source_tree", "source", "tree"], ["rustc_version", "toolchain", "rustc_version"], ["cargo_version", "toolchain", "cargo_version"]]) {
        assert.ok(recoveryJob.settings.includes(`      ${name}: \${{ steps.${step}.outputs.${field} }}`))
      }
    } else {
      assert.match(recoveryJob.settings, /^    needs: build$/mu)
    }
    assert.equal(recoveryJob.steps[0], `      - uses: actions/checkout@v7\n        with:\n          repository: bigduu/Bamboo-agent\n          ref: ${recoverySha}\n          persist-credentials: false`)
    const names = recoveryJob.steps.slice(1).map((step) => step.match(/^      - name: (.*)$/mu)?.[1])
    assert.deepEqual(names, [
      "Require the exact clean candidate source", "Setup Node.js", "Test original CI policy", "Install Rust",
      "Require and record the original Linux Rust toolchain", "Check formatting",
      ...(index === 0 ? ["Build locked Rust workspace"] : ["Compile locked Rust test targets", "Test locked Rust workspace"]),
      "Record checked source identity", "Require source files to remain unchanged",
    ])
    assert.equal(recoveryStep(recoveryJob, "Setup Node.js"), "      - name: Setup Node.js\n        uses: actions/setup-node@v7\n        with:\n          node-version: lts/*\n          package-manager-cache: false")
    assert.equal(recoveryStep(recoveryJob, "Install Rust"), "      - name: Install Rust\n        uses: dtolnay/rust-toolchain@stable\n        with:\n          toolchain: 1.99.0\n          components: rustfmt")
    for (const [name, command, timeout] of [
      ["Test original CI policy", "node --test scripts/ci-policy.test.cjs"],
      ["Check formatting", "cargo fmt --all -- --check"],
      ...(index === 0 ? [["Build locked Rust workspace", "cargo build --locked"]] : [
        ["Compile locked Rust test targets", "cargo test --locked --no-run", 15],
        ["Test locked Rust workspace", "cargo test --locked", 30],
      ]),
      ["Record checked source identity", "git rev-parse HEAD HEAD^{tree}"],
      ["Require source files to remain unchanged", "git diff --exit-code"],
    ]) {
      assert.equal(recoveryStep(recoveryJob, name), `      - name: ${name}\n${timeout ? `        timeout-minutes: ${timeout}\n` : ""}        run: ${command}${timeout ? '\n        env:\n          RUST_MIN_STACK: "8388608"' : ""}`)
    }
    const guard = recoveryStep(recoveryJob, "Require the exact clean candidate source")
    assert.equal(guard.split("        run: |\n")[0], "      - name: Require the exact clean candidate source\n" + (index === 0
      ? "        id: source\n"
      : "        env:\n          BUILD_SOURCE_SHA: ${{ needs.build.outputs.source_sha }}\n          BUILD_SOURCE_TREE: ${{ needs.build.outputs.source_tree }}\n") + "        shell: bash\n")
    for (const line of ['test "$source_sha" = "$EXPECTED_SOURCE_SHA"', 'test "$source_tree" = "$EXPECTED_SOURCE_TREE"', 'test -z "$(git status --porcelain --untracked-files=all)"']) {
      assert.ok(recoveryScript(guard).split("\n").includes(line), `missing source guard: ${line}`)
    }
    const toolchain = recoveryStep(recoveryJob, "Require and record the original Linux Rust toolchain")
    assert.equal(toolchain.split("        run: |\n")[0], "      - name: Require and record the original Linux Rust toolchain\n" + (index === 0
      ? "        id: toolchain\n"
      : "        env:\n          BUILD_RUSTC_VERSION: ${{ needs.build.outputs.rustc_version }}\n          BUILD_CARGO_VERSION: ${{ needs.build.outputs.cargo_version }}\n") + "        shell: bash\n")
    assert.match(toolchain, /test "\$rustc_version" = "rustc 1\.99\.0 \(b940084d7 2026-09-28\)"/u)
    assert.ok(toolchain.includes('= "b940084d7eb6a299eb4bfeb8e34901bc051e7ac4"'))
    assert.ok(toolchain.includes('= "x86_64-unknown-linux-gnu"'))
    if (index === 1) {
      for (const [variable, output, actual] of [["BUILD_SOURCE_SHA", "source_sha", "source_sha"], ["BUILD_SOURCE_TREE", "source_tree", "source_tree"], ["BUILD_RUSTC_VERSION", "rustc_version", "rustc_version"], ["BUILD_CARGO_VERSION", "cargo_version", "cargo_version"]]) {
        const step = variable.startsWith("BUILD_SOURCE") ? guard : toolchain
        assert.ok(step.includes(`          ${variable}: \${{ needs.build.outputs.${output} }}`))
        assert.ok(recoveryScript(step).split("\n").includes(`test "$${actual}" = "$${variable}"`))
      }
    }
  }
}

test("frozen Test recovery preserves both original command gates and exact-C identity", () => {
  assertRecoveryPolicy(recoveryWorkflow)
  for (const [from, to] of [
    ["  workflow_dispatch:", "  workflow_dispatch:\n    inputs:\n      source:\n        type: string"],
    ["contents: read", "contents: write"],
    ["env:\n", 'env:\n  RUST_TEST_THREADS: "1"\n'],
    ["env:\n", 'env:\n  CARGO_PROFILE_TEST_OPT_LEVEL: "3"\n'],
    ["    needs: build", '    env:\n      CARGO_PROFILE_TEST_OPT_LEVEL: "3"\n    needs: build'],
    ["        id: source", '        env:\n          RUST_TEST_THREADS: "1"\n        id: source'],
    [`ref: ${recoverySha}`, "ref: dev"],
    ["needs: build", "needs: []"],
    ["timeout-minutes: 45", "timeout-minutes: 60"],
    ["timeout-minutes: 15", "timeout-minutes: 30"],
    ["timeout-minutes: 30", "timeout-minutes: 45"],
    ["package-manager-cache: false", "package-manager-cache: true"],
    ["toolchain: 1.99.0", "toolchain: stable"],
    ["run: cargo build --locked", "run: true"],
    ["run: cargo test --locked\n", "run: cargo test --locked -- --skip slow\n"],
    ['RUST_MIN_STACK: "8388608"', 'RUST_MIN_STACK: "16777216"'],
    ["run: git diff --exit-code", "run: true"],
    ['test "$source_tree" = "$EXPECTED_SOURCE_TREE"', "true"],
    ['test "$source_sha" = "$BUILD_SOURCE_SHA"', "true"],
    ['test "$cargo_version" = "$BUILD_CARGO_VERSION"', "true"],
    ["        run: cargo test --locked --no-run", "        continue-on-error: true\n        run: cargo test --locked --no-run"],
    ["    needs: build", "    if: always()\n    needs: build"],
    ["      - name: Check formatting", "      - uses: actions/cache@v4\n      - name: Check formatting"],
    ["      - name: Require source files to remain unchanged", "      - name: Reset source\n        run: git reset --hard\n      - name: Require source files to remain unchanged"],
  ]) {
    assert.ok(recoveryWorkflow.includes(from), `mutation target exists: ${from}`)
    assert.throws(() => assertRecoveryPolicy(recoveryWorkflow.replace(from, to)), `reject unsafe recovery mutation: ${to}`)
  }
})

test("frozen Test recovery source guards reject drift and dirty entries on both jobs", () => {
  const directory = mkdtempSync(path.join(os.tmpdir(), "bamboo-recovery-source-"))
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
    for (const source of [recoveryWorkflow, recoveryWorkflow.replace(/\n/gu, "\r\n")]) {
      for (const [index, recoveryJob] of recoveryJobs(source).entries()) {
        const script = recoveryScript(recoveryStep(recoveryJob, "Require the exact clean candidate source"))
        const run = (env = {}) => spawnSync("bash", ["-e", "-o", "pipefail", "-c", script], {
          cwd: checkout, encoding: "utf8", env: { ...process.env,
            EXPECTED_SOURCE_SHA: sha, EXPECTED_SOURCE_TREE: tree,
            BUILD_SOURCE_SHA: sha, BUILD_SOURCE_TREE: tree,
            GITHUB_OUTPUT: path.join(directory, "outputs"), GITHUB_SHA: "controller-R",
            GITHUB_RUN_ID: "fixture-run", GITHUB_RUN_ATTEMPT: "1", ImageOS: "fixture", ImageVersion: "1",
            ...env,
          },
        })
        const accepted = run()
        assert.equal(accepted.status, 0, accepted.stderr)
        for (const env of [{ EXPECTED_SOURCE_SHA: "0".repeat(40) }, { EXPECTED_SOURCE_TREE: "1".repeat(40) },
          ...(index === 1 ? [{ BUILD_SOURCE_SHA: "" }, { BUILD_SOURCE_TREE: "2".repeat(40) }] : [])]) {
          assert.notEqual(run(env).status, 0, "mismatched or missing identity fails closed")
        }
        writeFileSync(path.join(checkout, ".cargo/config.toml"), "# dirty\n")
        assert.notEqual(run().status, 0, "tracked changes fail closed")
        git("checkout", "--", ".cargo/config.toml")
        writeFileSync(path.join(checkout, "untracked.txt"), "fixture\n")
        assert.notEqual(run().status, 0, "untracked changes fail closed")
        rmSync(path.join(checkout, "untracked.txt"))
        const finalDiff = recoveryStep(recoveryJob, "Require source files to remain unchanged").split("        run: ")[1]
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

test("frozen Test recovery toolchain guards reject changed compilers and cross-job tools", () => {
  const directory = mkdtempSync(path.join(os.tmpdir(), "bamboo-recovery-toolchain-"))
  const rustcVersion = "rustc 1.99.0 (b940084d7 2026-09-28)"
  const cargoVersion = "cargo 1.99.0 (fixture)"
  writeFileSync(path.join(directory, "rustc"), [
    "#!/bin/bash", 'printf "%s\\n" "$FIXTURE_RUSTC_VERSION"',
    'if [[ "$*" == "--version --verbose" ]]; then',
    '  printf "commit-hash: %s\\nhost: %s\\n" "$FIXTURE_RUSTC_HASH" "$FIXTURE_RUSTC_HOST"', "fi", "",
  ].join("\n"), { mode: 0o755 })
  writeFileSync(path.join(directory, "cargo"), '#!/bin/bash\nprintf "%s\\n" "$FIXTURE_CARGO_VERSION"\n', { mode: 0o755 })
  try {
    for (const [index, recoveryJob] of recoveryJobs(recoveryWorkflow).entries()) {
      const script = recoveryScript(recoveryStep(recoveryJob, "Require and record the original Linux Rust toolchain"))
      const run = (env = {}) => spawnSync("bash", ["-e", "-o", "pipefail", "-c", script], {
        encoding: "utf8", env: { ...process.env,
          PATH: `${directory}${path.delimiter}${process.env.PATH}`,
          GITHUB_OUTPUT: path.join(directory, "outputs"),
          FIXTURE_RUSTC_VERSION: rustcVersion, FIXTURE_CARGO_VERSION: cargoVersion,
          FIXTURE_RUSTC_HASH: "b940084d7eb6a299eb4bfeb8e34901bc051e7ac4",
          FIXTURE_RUSTC_HOST: "x86_64-unknown-linux-gnu",
          BUILD_RUSTC_VERSION: rustcVersion, BUILD_CARGO_VERSION: cargoVersion, ...env,
        },
      })
      const accepted = run()
      assert.equal(accepted.status, 0, accepted.stderr)
      for (const env of [{ FIXTURE_RUSTC_VERSION: "rustc 1.100.0" }, { FIXTURE_RUSTC_HASH: "0".repeat(40) },
        { FIXTURE_RUSTC_HOST: "aarch64-unknown-linux-gnu" },
        ...(index === 1 ? [{ BUILD_RUSTC_VERSION: "" }, { BUILD_CARGO_VERSION: "" }, { FIXTURE_CARGO_VERSION: "cargo changed" }] : [])]) {
        assert.notEqual(run(env).status, 0, "changed or unbound toolchain fails closed")
      }
    }
  } finally {
    rmSync(directory, { recursive: true, force: true })
  }
})
