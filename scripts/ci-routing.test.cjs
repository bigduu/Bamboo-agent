"use strict";
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { spawnSync } = require("node:child_process");
const { test } = require("node:test");
const { runInNewContext } = require("node:vm");
const read = file => fs.readFileSync(file, "utf8").replace(/\r\n/g, "\n");
const ci = read(".github/workflows/ci.yml");
const frontend = read(".github/workflows/frontend-acceptance.yml");
function job(source, id) {
  const marker = `\n  ${id}:\n`;
  const start = source.indexOf(marker); assert.notEqual(start, -1, `missing ${id}`);
  const tail = source.slice(start + marker.length), next = tail.search(/\n  [a-z][a-z0-9-]*:\n/);
  return next < 0 ? tail : tail.slice(0, next);
}
function script(step) {
  const match = step.match(/        run: \|\n([\s\S]*?)(?=\n      - |$)/);
  assert.ok(match);
  return match[1].split("\n").map(line => line.replace(/^          /, "")).join("\n");
}
const featureJobs = ["quick-checks", "feature-shards", "native-feature-tests", "arm64-feature", "browser-runtime-tests", "msrv", "lint", "docs", "security", "deny"];
function assertRouting(source) {
  const header = source.split("\njobs:\n")[0];
  assert.match(header, /pull_request:\n    branches: \[ dev, main \]/);
  assert.doesNotMatch(header, /^  push:/m);
  assert.match(job(source, "validation-source"), /if: github.event_name != 'pull_request' \|\| github.base_ref == 'dev'/);
  for (const id of featureJobs) {
    const block = job(source, id);
    assert.match(block, /^    needs: validation-source$/m);
    assert.match(block, /^    if: needs.validation-source.outputs.feature == 'true'/m);
    if (!["quick-checks", "native-feature-tests"].includes(id)) assert.match(block, /needs.validation-source.outputs.docs_only == 'false'/);
    assert.doesNotMatch(block, /continue-on-error:/);
  }
  assert.deepEqual([...source.matchAll(/^  ([a-z][a-z0-9-]*):$/gm)].map(row => row[1]).sort(),
    ["validation-source", "promotion-source", "test", "e2e-test", "feature-tests", ...featureJobs].sort());
  assert.match(job(source, "promotion-source"), /if: github.event_name == 'pull_request' && github.base_ref == 'main'/);
  assert.doesNotMatch(job(source, "promotion-source"), /actions\/checkout|cargo |node |contents: write/);
  for (const id of ["test", "e2e-test"]) {
    const block = job(source, id);
    assert.match(block, /if: always\(\) && !cancelled\(\) && \(github.event_name == 'workflow_dispatch' \|\| \(github.event_name == 'pull_request' && github.base_ref == 'dev'\)\)/);
    assert.doesNotMatch(block.split('    steps:')[0], /outputs\.feature/, 'a failed source gate must fail the real summary instead of skipping it');
    assert.doesNotMatch(block, /uses:|cargo |node |continue-on-error:/);
    assert.match(block, /test "\$FEATURE_RESULT" = success|FEATURE_RESULT NATIVE_RESULT/);
  }
  assert.equal((source.match(/run: cargo fmt --all -- --check/g) || []).length, 1);
  assert.equal((source.match(/run: node scripts\/run-feature-tests.cjs/g) || []).length, 1);
  assert.equal((source.match(/run: node scripts\/native-test-owner.cjs/g) || []).length, 1);
  assert.match(job(source, "native-feature-tests"), /github.event_name == 'workflow_dispatch'/);
  assert.match(job(source, "native-feature-tests"), /os: \[windows-2025, macos-latest\]/);
  const shards = job(source, "feature-shards");
  assert.match(shards, /fail-fast: false\n      matrix:\n        shard:/);
  assert.deepEqual([...shards.matchAll(/^          - ([a-z0-9-]+)$/gm)].map(match => match[1]), require("./run-feature-tests.cjs").FEATURE_SHARDS);
  assert.match(shards, /BAMBOO_FEATURE_SHARD: \$\{\{ matrix.shard \}\}/);
  const aggregate = job(source, "feature-tests");
  assert.match(aggregate, /needs: \[validation-source, feature-shards, arm64-feature\]/);
  assert.match(aggregate, /test "\$SHARDS_RESULT" = success/);
  assert.match(aggregate, /test "\$ARM64_RESULT" = success/);
  const arm64 = job(source, "arm64-feature");
  assert.match(arm64, /- os: ubuntu-22\.04-arm\n +target: aarch64-unknown-linux-gnu/);
  assert.match(arm64, /- os: windows-11-arm\n +target: aarch64-pc-windows-msvc/);
  assert.match(arm64, /architecture: arm64/);
  assert.match(arm64, /test "\$RUNNER_ARCH" = ARM64/);
  assert.match(arm64, /assert.equal\(nativeTarget\(\), target/);
  assert.match(arm64, /cargo test --locked --target "\$BAMBOO_TEST_TARGET" --test frontend_packaging -- --exact/);
  assert.match(arm64, /grep -Fx "test \$case_name \.\.\. ok"/);
  assert.match(arm64, /binaryArchitecture\(path.join/);
  assert.match(job(source, "msrv"), /cargo check --locked --workspace --all-targets --all-features/);
  assert.match(job(source, "lint"), /cargo clippy --all-features --/);
  assert.match(job(source, "security"), /cargo audit --deny warnings/);
  assert.match(job(source, "deny"), /command: check/);
  const browser = job(source, "browser-runtime-tests");
  assert.match(browser, /name: Browser Runtime Unit and E2E \(\$\{\{ matrix.shard \}\}\)/);
  assert.match(browser, /strategy:\n      fail-fast: false\n      matrix:\n        shard: \[host, profile, shared, other\]/);
  assert.match(browser, /cli.js install --with-deps chromium-headless-shell/);
  assert.match(browser, /readdirSync\('browser-runtime'\)\.filter\(file => file\.endsWith\('\.test\.cjs'\)\)/);
  assert.match(browser, /spawnSync\(process.execPath, \['--test', '--test-reporter=tap', '--test-concurrency=1', \.\.\.files\]/);
  assert.match(browser, /for \(const name of \['fail', 'cancelled', 'skipped', 'todo'\]\)/);
  assert.match(browser, /BROWSER_RUNTIME_SHARD: \$\{\{ matrix.shard \}\}/);
  assert.match(browser, /name: browser-runtime-\$\{\{ matrix.shard \}\}-\$\{\{ github.run_id \}\}-\$\{\{ github.run_attempt \}\}/);
  assert.ok(job(source, "test").includes('test "${!key}" = success'));
  assert.match(job(source, "e2e-test"), /test "\$BROWSER_RESULT" = success/);
  assert.doesNotMatch(source, /outputs.full == 'true'|cargo build[^\n]*--release/);
}
test("feature CI owns tests and quality once; main has only read-only promotion", () => {
  assertRouting(ci);
  for (const [before, after] of [
    ["  workflow_dispatch:", "  push:\n    branches: [main]\n  workflow_dispatch:"],
    ["needs.validation-source.outputs.feature == 'true'", "always()"],
    ["run: node scripts/run-feature-tests.cjs", "run: cargo test"],
    ["          - agent-tests-0\n", ""],
    ["shard: [host, profile, shared, other]", "shard: [host, profile, shared]"],
    ["run: cargo fmt --all -- --check", "run: true"],
  ]) assert.throws(() => assertRouting(ci.replace(before, after)));
});

test("browser matrix executes a complete disjoint file selection and preserves child isolation", () => {
  const inventory = fs.readdirSync("browser-runtime").filter(file => file.endsWith(".test.cjs")).sort();
  const run = (shard, { source = ci, files = inventory, status = 0 } = {}) => {
    const block = job(source, "browser-runtime-tests");
    const match = block.match(/node <<'NODE' \| tee[^\n]*\n([\s\S]*?)\n          NODE/);
    assert.ok(match, "missing actual runtime selector");
    const calls = [];
    let exit;
    const fakeRequire = name => {
      if (name === "node:fs") return { readdirSync(directory) { assert.equal(directory, "browser-runtime"); return [...files]; } };
      if (name === "node:child_process") return { spawnSync(executable, args, options) {
        calls.push({ executable, args, options }); return { status };
      } };
      return require(name);
    };
    const environment = { PATH: "synthetic-path", PLAYWRIGHT_BROWSERS_PATH: "/synthetic/browser", BROWSER_RUNTIME_SHARD: shard,
      GITHUB_SHA: "a".repeat(40), GITHUB_TOKEN: "denied", ACTIONS_RUNTIME_TOKEN: "denied", GITHUB_OUTPUT: "/denied/output" };
    new Function("require", "process", "console", match[1])(fakeRequire,
      { execPath: process.execPath, env: environment, exit(code) { exit = code; } }, { log() {} });
    return { calls, exit };
  };
  const selected = [];
  for (const shard of ["host", "profile", "shared", "other"]) {
    const result = run(shard);
    assert.equal(result.exit, 0); assert.equal(result.calls.length, 1);
    const { executable, args, options } = result.calls[0];
    assert.equal(executable, process.execPath);
    assert.deepEqual(args.slice(0, 3), ["--test", "--test-reporter=tap", "--test-concurrency=1"]);
    assert.ok(args.length > 3); selected.push(...args.slice(3));
    assert.equal(options.cwd, "browser-runtime"); assert.equal(options.stdio, "inherit");
    assert.deepEqual(Object.keys(options.env).sort(), ["GITHUB_SHA", "PATH", "PLAYWRIGHT_BROWSERS_PATH"]);
    if (shard !== "other") assert.deepEqual(args.slice(3), [{ host: "host.test.cjs", profile: "profile.test.cjs", shared: "shared-host.test.cjs" }[shard]]);
  }
  assert.deepEqual(selected.sort(), inventory); assert.equal(new Set(selected).size, selected.length);
  assert.equal(run("host", { status: 17 }).exit, 17, "runtime failure must fail its shard");
  assert.throws(() => run("unknown"), /Unknown runtime shard/);
  assert.throws(() => run("host", { files: inventory.filter(file => file !== "profile.test.cjs") }), /cover every file exactly once/);
  assert.throws(() => run("host", { files: [...inventory, inventory[0]] }), /Duplicate runtime file/);
  assert.throws(() => run("host", { files: ["host.test.cjs", "profile.test.cjs", "shared-host.test.cjs"] }), /Empty runtime shard/);
  assert.throws(() => run("host", { source: ci.replace("shared: [browserFiles[2]]", "shared: [browserFiles[0]]") }), /cover every file exactly once/);
});

test("feature source controller preserves the official log and rejects main or release execution", t => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "bamboo-ci-routing-"));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const shell = script(job(ci, "validation-source")), sha = "a".repeat(40);
  const run = (overrides = {}) => {
    const output = path.join(directory, "output"); fs.writeFileSync(output, "");
    const result = spawnSync("bash", ["-c", shell], { encoding: "utf8", env: { ...process.env,
      GITHUB_EVENT_NAME: "workflow_dispatch", GITHUB_SHA: sha, EXPECTED_SNAPSHOT_SHA: sha,
      GITHUB_REF: "refs/heads/dev", GITHUB_BASE_REF: "", GITHUB_OUTPUT: output, ...overrides } });
    return { ...result, output: fs.readFileSync(output, "utf8") };
  };
  for (const overrides of [{}, { GITHUB_EVENT_NAME: "pull_request", GITHUB_BASE_REF: "dev" }]) {
    const result = run(overrides); assert.equal(result.status, 0, result.stderr);
    assert.equal(result.output, "full=false\nfeature=true\n");
    assert.equal(result.stdout, `Event source: ${sha}; full validation: false\n`);
  }
  for (const overrides of [
    { EXPECTED_SNAPSHOT_SHA: "" }, { EXPECTED_SNAPSHOT_SHA: "dev" }, { EXPECTED_SNAPSHOT_SHA: "b".repeat(40) },
    { EXPECTED_SNAPSHOT_SHA: "A".repeat(40) }, { EXPECTED_SNAPSHOT_SHA: `${sha}\nfeature=true` },
    { EXPECTED_SNAPSHOT_SHA: "$(touch injected)" }, { GITHUB_SHA: "" },
    { GITHUB_REF: "refs/heads/main" }, { GITHUB_REF: "refs/heads/release/" + sha },
    { GITHUB_EVENT_NAME: "push" }, { GITHUB_EVENT_NAME: "pull_request", GITHUB_BASE_REF: "main" },
    { GITHUB_EVENT_NAME: "pull_request", GITHUB_BASE_REF: "integration/batch" },
    { GITHUB_EVENT_NAME: "pull_request_target" }, { GITHUB_EVENT_NAME: "unknown" },
  ]) { const result = run(overrides); assert.notEqual(result.status, 0); assert.equal(result.output, ""); }
});

test("documentation routing uses the actual merge and never skips embedded Markdown", t => {
  const { documentationOnly } = require("./ci-change-scope.cjs");
  assert.equal(documentationOnly(["README.md", "docs/nested/guide.md"]), true);
  for (const files of [[], ["crates/infra/builtin/explorer.md"], ["docs/guide.md", "src/main.rs"], [".github/workflows/ci.yml"], ["docs/tool.sh"]]) {
    assert.equal(documentationOnly(files), false);
  }
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "bamboo-ci-scope-"));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const git = (...args) => {
    const result = spawnSync("git", args, { cwd: directory, encoding: "utf8" });
    assert.equal(result.status, 0, result.stderr); return result.stdout.trim();
  };
  git("init", "-q", "-b", "main");
  git("config", "user.name", "CI fixture"); git("config", "user.email", "ci@example.invalid");
  fs.writeFileSync(path.join(directory, "README.md"), "base\n");
  fs.writeFileSync(path.join(directory, "source.rs"), "source\n");
  fs.mkdirSync(path.join(directory, "docs"));
  git("add", "."); git("commit", "-qm", "base"); const base = git("rev-parse", "HEAD");
  git("checkout", "-qb", "docs-only"); fs.writeFileSync(path.join(directory, "README.md"), "docs\n");
  git("commit", "-qam", "docs"); git("checkout", "-q", "main"); git("merge", "--no-ff", "-qm", "docs merge", "docs-only");
  const firstMerge = git("rev-parse", "HEAD");
  const classify = (before, after) => spawnSync(process.execPath, [path.resolve("scripts/ci-change-scope.cjs"), before, after], { cwd: directory, encoding: "utf8" });
  assert.equal(classify(base, firstMerge).stdout.trim(), "true");
  git("checkout", "-qb", "renamed-source"); git("mv", "source.rs", "docs/source.md");
  git("commit", "-qm", "rename source"); git("checkout", "-q", "main"); git("merge", "--no-ff", "-qm", "rename merge", "renamed-source");
  const secondMerge = git("rev-parse", "HEAD");
  assert.equal(classify(firstMerge, secondMerge).stdout.trim(), "false");
  assert.notEqual(classify(base, secondMerge).status, 0);
});

test("required feature summaries reject every unsuccessful selected owner", () => {
  const defaults = { SOURCE_RESULT: "success", FEATURE_VALIDATION: "true", DOCS_ONLY: "false", NATIVE_REQUESTED: "false", GITHUB_EVENT_NAME: "pull_request", GITHUB_BASE_REF: "dev" };
  const fields = ["QUICK_RESULT", "FEATURE_RESULT", "NATIVE_RESULT", "BROWSER_RESULT", "LINT_RESULT", "MSRV_RESULT", "DOCS_RESULT", "SECURITY_RESULT", "DENY_RESULT"];
  for (const field of fields) defaults[field] = "success";
  defaults.NATIVE_RESULT = "skipped";
  const run = (id, overrides = {}) => spawnSync("bash", ["-c", script(job(ci, id))], { encoding: "utf8", env: { ...process.env, ...defaults, ...overrides } });
  assert.equal(run("test").status, 0);
  assert.equal(run("test", { GITHUB_EVENT_NAME: "workflow_dispatch", GITHUB_BASE_REF: "" }).status, 0);
  assert.equal(run("test", { GITHUB_EVENT_NAME: "workflow_dispatch", GITHUB_BASE_REF: "", NATIVE_REQUESTED: "true", NATIVE_RESULT: "success" }).status, 0);
  assert.notEqual(run("test", { GITHUB_EVENT_NAME: "workflow_dispatch", GITHUB_BASE_REF: "", NATIVE_REQUESTED: "true" }).status, 0);
  for (const field of ["SOURCE_RESULT", ...fields.filter(field => field !== "NATIVE_RESULT")]) for (const value of ["failure", "cancelled", "skipped", "", "unknown"]) {
    assert.notEqual(run("test", { [field]: value }).status, 0, `${field}:${value}`);
  }
  for (const value of ["failure", "cancelled", "success", "", "unknown"]) assert.notEqual(run("test", { NATIVE_RESULT: value }).status, 0);
  const documentation = Object.fromEntries(fields.filter(field => field !== "QUICK_RESULT").map(field => [field, "skipped"]));
  assert.equal(run("test", { ...documentation, DOCS_ONLY: "true" }).status, 0);
  assert.equal(run("e2e-test", { ...documentation, DOCS_ONLY: "true" }).status, 0);
  assert.notEqual(run("test", { ...documentation, DOCS_ONLY: "true", FEATURE_RESULT: "failure" }).status, 0);
  assert.notEqual(run("test", { ...documentation, DOCS_ONLY: "true", GITHUB_EVENT_NAME: "workflow_dispatch", GITHUB_BASE_REF: "" }).status, 0);
  for (const overrides of [{ FEATURE_VALIDATION: "false" }, { GITHUB_BASE_REF: "main" }, { GITHUB_EVENT_NAME: "push" }]) assert.notEqual(run("test", overrides).status, 0);
  assert.equal(run("e2e-test").status, 0);
  const shards = { SOURCE_RESULT: "success", SHARDS_RESULT: "success", ARM64_RESULT: "success" };
  const runFeature = (arm64) => spawnSync("bash", ["-c", script(job(ci, "feature-tests"))],
    { env: { ...process.env, ...shards, ARM64_RESULT: arm64 } });
  assert.equal(runFeature("success").status, 0);
  for (const value of ["failure", "cancelled", "skipped", "", "unknown"]) assert.notEqual(runFeature(value).status, 0);
  for (const field of ["FEATURE_RESULT", "BROWSER_RESULT"]) for (const value of ["failure", "cancelled", "skipped", "", "unknown"]) assert.notEqual(run("e2e-test", { [field]: value }).status, 0);
  for (const value of ["failure", "cancelled", "success", "", "unknown"]) assert.notEqual(run("e2e-test", { NATIVE_RESULT: value }).status, 0);
});

test("standalone duplicate acceptances are manual and unique isolation and CodeQL remain on feature", () => {
  for (const name of ["frontend-acceptance.yml", "actor-snapshot-windows.yml", "ticket-platform-adapter.yml"]) {
    const source = read(".github/workflows/" + name), header = source.split("\njobs:\n")[0];
    assert.match(header, /on:\n  workflow_dispatch:/);
    assert.doesNotMatch(header, /pull_request:|push:/);
  }
  const codeql = read(".github/workflows/codeql.yml");
  assert.match(codeql, /pull_request:\n    branches: \[ dev \]/);
  assert.match(codeql, /language: \[ actions, javascript-typescript, python, rust \]/);
  assert.match(codeql, /build-mode: none/);
  const isolation = read(".github/workflows/release-cargo-isolation.yml");
  assert.match(isolation, /pull_request:\n    branches: \[dev\]/);
  assert.doesNotMatch(isolation, /branches: \[[^\]]*main/);
  assert.doesNotMatch(isolation, /node --test[^\n]*(?:release-frontend-policy|lotus-next-artifact)/, "Quick Checks already owns these suites");
  // These accepted joint/API-only configurations are intentionally distinct.
  for (const name of ["trusted-observer.yml", "delegated-planner-acceptance.yml", "observer-subscriptions.yml"]) assert.match(read(".github/workflows/" + name), /BAMBOO_FRONTEND_BUILD_MODE: api-only/);
});

test("frontend source acceptance retains the exact-head build and focused frontend tests", () => {
  assert.doesNotMatch(frontend.split("\njobs:\n")[0], /pull_request:|push:/);
  assert.doesNotMatch(frontend, /branches: \[dev\]|paths:/);
  assert.match(frontend, /snapshot_sha:\n.*\n        required: true\n        type: string/);
  for (const id of ["lint", "build", "test"]) {
    const block = job(frontend, id);
    assert.match(block, /ref: \$\{\{ github.event_name == 'pull_request' && github.event.pull_request.head.sha \|\| github.sha \}\}/);
    assert.match(block, /test "\$EXPECTED_SNAPSHOT_SHA" = "\$GITHUB_SHA"/);
    assert.match(block, /source_status="\$\(git status --porcelain --untracked-files=all\)"/);
    assert.match(block, /test -z "\$source_status"/);
    assert.doesNotMatch(block, /continue-on-error:/);
  }
  assert.ok(job(frontend, "build").includes("        run: cargo build --locked\n"));
  for (const mode of ["--no-run", "--no-fail-fast"]) {
    for (const target of [
      "-p bamboo-agent --test frontend_packaging",
      "-p bamboo-server --test frontend_build_contract",
      "-p bamboo-server --lib services::frontend_package::tests",
    ]) assert.ok(job(frontend, "test").includes(`          cargo test --locked ${mode} ${target}\n`));
  }
  assert.doesNotMatch(job(frontend, "test"), /^\s*cargo test --locked --(?:no-run|no-fail-fast)\s*$/m);
  assert.match(job(frontend, "lint"), /cargo clippy --all-features --/);
});

test("source guards propagate Git inspection errors and reject dirty or mismatched sources", (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "bamboo-ci-source-"));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const repo = path.join(directory, "repo"); const bin = path.join(directory, "bin");
  fs.mkdirSync(repo); fs.mkdirSync(bin); fs.writeFileSync(path.join(repo, "Cargo.lock"), "# fixture\n");
  const git = (...args) => {
    const result = spawnSync("git", args, { cwd: repo, encoding: "utf8" });
    assert.equal(result.status, 0, result.stderr); return result.stdout.trim();
  };
  const realGit = spawnSync("which", ["git"], { encoding: "utf8" }).stdout.trim();
  git("init", "-q"); git("add", ".");
  git("-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-qm", "fixture");
  const sha = git("rev-parse", "HEAD");
  const tree = git("rev-parse", "HEAD^{tree}");
  fs.writeFileSync(path.join(bin, "git"), '#!/bin/bash\nif [[ "$1" == status && "$FAIL_STATUS" == 1 ]]; then echo "fatal: inspection failed" >&2; exit 23; fi\nexec "$REAL_GIT" "$@"\n', { mode: 0o755 });
  const guards = [];
  for (const source of [job(frontend, "lint"), job(frontend, "build"), job(frontend, "test")]) {
    for (const step of source.split(/(?=^      - )/m)) {
      if (step.includes("source_status=")) guards.push(script(step));
    }
  }
  assert.equal(guards.length, 5);
  for (const shell of guards) {
    const run = (overrides = {}) => spawnSync("bash", ["-c", shell], { cwd: repo, encoding: "utf8", env: { ...process.env,
      PATH: `${bin}${path.delimiter}${process.env.PATH}`, REAL_GIT: realGit, FAIL_STATUS: "0",
      GITHUB_SHA: sha, EXPECTED_SHA: sha, EXPECTED_TREE: tree,
      BUILD_SOURCE_SHA: sha, BUILD_SOURCE_TREE: tree, GITHUB_OUTPUT: path.join(directory, "outputs"),
      GITHUB_EVENT_NAME: "workflow_dispatch", EXPECTED_SNAPSHOT_SHA: sha, ...overrides } });
    assert.equal(run().status, 0);
    assert.notEqual(run({ FAIL_STATUS: "1" }).status, 0, "Git status failure must not be clean success");
    assert.notEqual(run({ GITHUB_SHA: "b".repeat(40), EXPECTED_SHA: "b".repeat(40) }).status, 0);
    fs.writeFileSync(path.join(repo, "untracked"), "dirty\n"); assert.notEqual(run().status, 0); fs.unlinkSync(path.join(repo, "untracked"));
    fs.writeFileSync(path.join(repo, "Cargo.lock"), "# changed\n"); assert.notEqual(run().status, 0);
    git("add", "Cargo.lock"); assert.notEqual(run().status, 0); git("reset", "--hard", sha);
  }
});

const pythonBody = (body, delimiter) => body.split(`<<'${delimiter}'\n`)[1].split(`\n${delimiter}`)[0];

test("main deploys only the retained accepted feature site and never recompiles missing docs", t => {
  const docs = read(".github/workflows/docs.yml"), producer = job(ci, "docs");
  assert.doesNotMatch(docs, /cargo |rust-toolchain|rust-cache|pull_request:/);
  assert.match(producer, /cargo doc --no-deps --all-features --document-private-items/);
  assert.match(producer, /name: documentation-\$\{\{ github.run_id \}\}-\$\{\{ github.run_attempt \}\}/);
  const steps = docs.split(/(?=^      - )/m), locate = pythonBody(script(steps.find(s => s.includes("name: Locate the accepted"))), "PYDOC");
  const verify = pythonBody(script(steps.find(s => s.includes("name: Verify the accepted"))), "PYDOC");
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "bamboo-ci-docs-"));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  fs.mkdirSync(path.join(directory, "doc"));
  const repository = "bigduu/Bamboo-agent", main = "a".repeat(40), cut = "b".repeat(40), head = "c".repeat(40), tree = "d".repeat(40);
  const pr = (base, merge, ref, sha) => ({ number: 29, merged_at: "2026-10-28T01:00:00Z", merge_commit_sha: merge, base: { ref: base }, head: { ref, sha, repo: { full_name: repository } } });
  const routes = {
    [`commits/${main}/pulls?per_page=100`]: [pr("main", main, "release/" + cut, cut)],
    [`commits/${cut}/pulls?per_page=100`]: [pr("dev", cut, "feature/example", head)],
    [`actions/workflows/ci.yml/runs?event=pull_request&head_sha=${head}&status=success&per_page=100`]: { workflow_runs: [{ id: 123, run_attempt: 2, head_sha: head, event: "pull_request", conclusion: "success", head_repository: { full_name: repository }, updated_at: "2026-10-28T00:59:00Z" }] },
    "actions/runs/123/artifacts?per_page=100": { total_count: 1, artifacts: [{ name: "documentation-123-1", expired: false }] },
  };
  const output = path.join(directory, "outputs"), env = { ...process.env, GITHUB_REPOSITORY: repository, GITHUB_SHA: main, GITHUB_OUTPUT: output,
    EXPECTED_RUN: "123", EXPECTED_ATTEMPT: "1", EXPECTED_PR: "29", EXPECTED_HEAD: head, EXPECTED_TREE: tree };
  const run = (body, model = routes) => spawnSync("python3", ["-c", `import json, subprocess, sys\npayload=json.load(sys.stdin)\ndef read(args, **kwargs):\n    if args[0]=='gh': return json.dumps(payload['routes'][args[-1].split('/', 3)[3]])\n    assert args==['git', 'rev-parse', '${cut}^{tree}']\n    return '${tree}\\n'\nsubprocess.check_output=read\nsubprocess.check_call=lambda args: 0 if args==['git', 'merge-base', '--is-ancestor', '${cut}', '${main}'] else sys.exit(1)\nexec(compile(payload['body'], '<actual-docs-workflow>', 'exec'))`], { cwd: directory, env, encoding: "utf8", input: JSON.stringify({ body, routes: model }) });
  assert.equal(run(locate).status, 0);
  assert.match(fs.readFileSync(output, "utf8"), /run=123\nattempt=1\nartifact=documentation-123-1/);
  for (const mutate of [
    model => { model[`commits/${main}/pulls?per_page=100`][0].head.sha = head; },
    model => { model[`commits/${cut}/pulls?per_page=100`] = []; },
    model => { Object.values(model).find(value => value.workflow_runs).workflow_runs[0].conclusion = "failure"; },
    model => { model["actions/runs/123/artifacts?per_page=100"].artifacts[0].expired = true; },
    model => { model["actions/runs/123/artifacts?per_page=100"].artifacts = []; },
  ]) { const model = structuredClone(routes); mutate(model); assert.notEqual(run(locate, model).status, 0); }
  const original = { GITHUB_REPOSITORY: repository, GITHUB_SHA: "e".repeat(40), GITHUB_RUN_ID: "123", GITHUB_RUN_ATTEMPT: "1", PR_NUMBER: "29", PR_HEAD_SHA: head, tree };
  const check = source => { fs.writeFileSync(path.join(directory, "doc/_bamboo-ci-source.json"), JSON.stringify(source)); return run(verify); };
  assert.equal(check(original).status, 0);
  for (const field of Object.keys(original)) assert.notEqual(check({ ...original, [field]: "changed" }).status, 0, field);
});

test("main promotion requires an unchanged cut and the complete frozen release contract", () => {
  const body = pythonBody(script(job(ci, "promotion-source")), "PYTHON"), sha = "a".repeat(40), dev = "b".repeat(40);
  const base = "c".repeat(40), merge = "d".repeat(40), tree = "e".repeat(40);
  const release = { id: 4, tag_name: "v2026.10.28", target_commitish: sha, draft: false, prerelease: false, published_at: "2026-10-28T00:00:00Z",
    assets: ["bamboo-aarch64-apple-darwin", "bamboo-x86_64-unknown-linux-gnu", "bamboo-x86_64-pc-windows-msvc.exe", "bamboo-sidecars-v2.json", "bamboo-executables-provenance-v2.json", "release-provenance.json"].map(name => ({ name, state: "uploaded", size: 1 })) };
  const policy = version => {
    const bytes = Buffer.from("// BAMBOO_SHARED_EXECUTABLES_V" + version + "\n");
    return { type: "file", encoding: "base64", size: bytes.length, content: bytes.toString("base64") };
  };
  const policyRoute = "contents/scripts/sidecar-release.cjs?ref=" + sha;
  const routes = { [policyRoute]: policy(2), [`branches/release%2F${sha}`]: { name: "release/" + sha, commit: { sha } }, "branches/dev": { name: "dev", protected: true, commit: { sha: dev } },
    "branches/main": { name: "main", commit: { sha: base } },
    [`git/commits/${merge}`]: { sha: merge, parents: [{ sha: base }, { sha }], tree: { sha: tree } },
    [`git/commits/${sha}`]: { sha, tree: { sha: tree } },
    [`compare/${sha}...${sha}`]: { status: "identical", base_commit: { sha }, merge_base_commit: { sha } },
    [`compare/${sha}...${dev}`]: { status: "ahead", base_commit: { sha }, merge_base_commit: { sha } },
    "releases?per_page=100": [release], "releases/4": release, "git/ref/tags/v2026.10.28": { object: { type: "commit", sha } } };
  const run = (model = routes, override = {}) => spawnSync("python3", ["-c", "import io,json,sys,urllib.request\npayload=json.load(sys.stdin)\nclass Opener:\n def open(self,request,timeout):\n  route=request.full_url.split('/',6)[6]\n  value=payload['routes'][route]\n  if route.startswith('branches/') and isinstance(value,list): value=value.pop(0)\n  return io.StringIO(json.dumps(value))\nurllib.request.build_opener=lambda *args: Opener()\nexec(compile(payload['body'],'<actual-promotion-workflow>','exec'))"], { encoding: "utf8", input: JSON.stringify({ body, routes: model }), env: { ...process.env, GITHUB_REPOSITORY: "bigduu/Bamboo-agent", GITHUB_SHA: merge, BASE_SHA: base, HEAD_REPOSITORY: "bigduu/Bamboo-agent", GH_TOKEN: "inert-fixture", HEAD_REF: "release/" + sha, HEAD_SHA: sha, ...override } });
  assert.equal(run().status, 0);
  const expanded = structuredClone(routes);
  expanded[policyRoute] = policy(3);
  assert.notEqual(run(expanded).status, 0, "A v2 asset set must not admit a v3 cut");
  expanded["releases/4"].assets = expanded["releases/4"].assets.map(asset => ({ ...asset, name: asset.name.replace(/-v2\.json$/, "-v3.json") }));
  for (const name of ["bamboo-aarch64-unknown-linux-gnu", "bamboo-aarch64-pc-windows-msvc.exe", "bamboo-browser-host-v1.tar.gz"]) {
    expanded["releases/4"].assets.push({ name, state: "uploaded", size: 1 });
  }
  assert.equal(run(expanded).status, 0);
  for (const asset of expanded["releases/4"].assets) {
    const missing = structuredClone(expanded);
    missing["releases/4"].assets = missing["releases/4"].assets.filter(value => value.name !== asset.name);
    assert.match(run(missing).stderr, /Completed release asset is missing/);
  }
  assert.match(run({ ...routes, [policyRoute]: policy(4) }).stderr, /Unknown frozen executable contract/);
  assert.notEqual(run(routes, { HEAD_SHA: dev }).status, 0);
  for (const mutate of [model => { model["releases/4"].draft = true; }, model => { model["releases/4"].assets.pop(); },
    model => { model["git/ref/tags/v2026.10.28"].object.sha = dev; }, model => { model["branches/dev"].protected = false; }]) {
    const model = structuredClone(routes); mutate(model); assert.notEqual(run(model).status, 0);
  }
  for (const [name, mutate, reason] of [
    ["main content survives the merge", m => { m[`git/commits/${merge}`].tree.sha = dev; }, /merge tree differs/],
    ["wrong base parent", m => { m[`git/commits/${merge}`].parents[0].sha = dev; }, /merge parents/],
    ["wrong release parent", m => { m[`git/commits/${merge}`].parents[1].sha = dev; }, /merge parents/],
    ["reversed parents", m => { m[`git/commits/${merge}`].parents.reverse(); }, /merge parents/],
    ["missing parent", m => { m[`git/commits/${merge}`].parents.pop(); }, /merge parents/],
    ["foreign merge response", m => { m[`git/commits/${merge}`].sha = dev; }, /commit identity/],
    ["foreign cut response", m => { m[`git/commits/${sha}`].sha = dev; }, /commit identity/],
    ["main already moved", m => { m["branches/main"].commit.sha = dev; }, /main no longer matches/],
    ["main moves during proof", m => { m["branches/main"] = [m["branches/main"], { name: "main", commit: { sha: dev } }]; }, /main changed during/],
    ["release moves during proof", m => { m[`branches/release%2F${sha}`] = [m[`branches/release%2F${sha}`], { name: "release/" + sha, commit: { sha: dev } }]; }, /Release branch changed/],
  ]) {
    const model = structuredClone(routes); mutate(model); const result = run(model);
    assert.notEqual(result.status, 0, name); assert.match(result.stderr, reason, name);
  }
  for (const override of [{ BASE_SHA: "" }, { GITHUB_SHA: "" }]) assert.notEqual(run(routes, override).status, 0);
});

test("native executable transport retries the same files only after a failed upload", () => {
  const native = job(read(".github/workflows/publish-crate.yml"), "executables");
  const steps = native.split("\n      - ").slice(1);
  const transport = steps.find(step => step.startsWith("name: Transport this platform's verified executable"));
  const retry = steps.find(step => step.startsWith("name: Retry transport of the verified executable"));
  assert.ok(transport && retry, "Require the first upload and its retry");
  assert.equal(steps.indexOf(retry), steps.indexOf(transport) + 1);
  assert.match(transport, /\n        id: native_transport\n        continue-on-error: true/);
  assert.match(transport, /\n        with: &verified-native-output\n          name: bamboo-executable-\$\{\{ matrix\.target \}\}-attempt-\$\{\{ github\.run_attempt \}\}/);
  assert.match(transport, /\n          path: \$\{\{ runner\.temp \}\}\/bamboo-executable-output\/\*/);
  assert.match(transport, /\n          overwrite: false/);
  const action = step => step.match(/\n        uses: (actions\/upload-artifact@[a-f0-9]{40}) /)?.[1];
  assert.equal(steps.filter(action).length, 2);
  assert.ok(action(transport));
  assert.equal(action(retry), action(transport));
  assert.match(retry, /\n        if: steps\.native_transport\.outcome == 'failure'\n/);
  assert.match(retry, /\n        with: \*verified-native-output/);
  assert.doesNotMatch(retry, /continue-on-error:|\n        run:/);
});

test("Docker receives the completed publisher's matching run and attempt", () => {
  const publisher = read(".github/workflows/publish-crate.yml");
  const docker = read(".github/workflows/docker-publish.yml");
  assert.match(publisher, /name: bamboo-completed-release-\$\{\{ github\.run_id \}\}-\$\{\{ github\.run_attempt \}\}/);
  assert.match(docker, /name: bamboo-completed-release-\$\{\{ github\.event\.workflow_run\.id \}\}-\$\{\{ github\.event\.workflow_run\.run_attempt \}\}/);
  assert.match(docker.split("\njobs:\n")[0], /workflow_run:\n    workflows: \[Publish Crate\]\n    types: \[completed\]\n    branches: \[dev\]/);
  assert.doesNotMatch(docker.split("\njobs:\n")[0], /^  push:/m);
  assert.match(docker, /contents\/scripts\/docker-release\.py\?ref=\$\{GITHUB_WORKFLOW_SHA\}/);
  assert.match(read("scripts/docker-release.py"), /executable\['digest'\] == 'sha256:' \+ entry\['sha256'\]/);
  assert.doesNotMatch(read("docker/Dockerfile.published"), /\b(?:cargo|rustc|vite) (?:build|run|install)\b/i);
  const arm = read("docker/Dockerfile");
  assert.match(read("Cargo.toml"), /libduckdb-sys = \{ path = "third_party\/libduckdb-sys" \}/);
  const patchCopy = arm.indexOf("COPY --from=planner /app/third_party/libduckdb-sys");
  assert.ok(patchCopy >= 0 && patchCopy < arm.indexOf("RUN cargo chef cook"),
    "The local Cargo patch must exist in the dependency stage before chef cook");
});

test("automatic Docker releases restore cache without attempting a read-only export", () => {
  const docker = read(".github/workflows/docker-publish.yml");
  assert.match(docker, /cache-from: type=gha,scope=\$\{\{ matrix\.platform \}\}/);
  const matches = [...docker.matchAll(/^\s+cache-to: \$\{\{ (.+) \}\}$/gm)];
  assert.equal(matches.length, 1, "Require one event-scoped cache export");
  const select = event_name => runInNewContext(matches[0][1], {
    github: { event_name }, matrix: { platform: "linux/amd64" },
    format: (value, platform) => value.replace("{0}", platform),
  });
  assert.equal(select("workflow_run"), "");
  assert.equal(select("workflow_dispatch"), "type=gha,mode=max,scope=linux/amd64,ignore-error=true");
});
