"use strict";
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { spawnSync } = require("node:child_process");
const { test } = require("node:test");
const ci = fs.readFileSync(".github/workflows/ci.yml", "utf8");
const frontend = fs.readFileSync(".github/workflows/frontend-acceptance.yml", "utf8");
const publisher = fs.readFileSync(".github/workflows/publish-crate.yml", "utf8");
function job(source, id) {
  const marker = `\n  ${id}:\n`;
  const start = source.indexOf(marker); assert.notEqual(start, -1);
  const tail = source.slice(start + marker.length);
  const next = tail.search(/\n  [a-z][a-z0-9-]*:\n/);
  return next < 0 ? tail : tail.slice(0, next);
}
function script(step) {
  const match = step.match(/        run: \|\n([\s\S]*?)(?=\n      - |$)/);
  assert.ok(match);
  return match[1].split("\n").map((line) => line.replace(/^          /, "")).join("\n");
}
function assertRouting(source) {
  const quick = job(source, "quick-checks");
  assert.match(quick, /^    name: Quick Checks$/m);
  assert.match(quick, /^    if: github.event_name == 'pull_request' && github.base_ref == 'dev'$/m);
  assert.match(quick, /run: cargo fmt --all -- --check/);
  assert.match(quick, /run: node --test scripts\/ci-policy.test.cjs scripts\/crate-release.test.cjs scripts\/ci-routing.test.cjs/);
  assert.doesNotMatch(quick, /cargo (build|test|check|clippy)|continue-on-error|--test-skip/);
  const full = job(source, "test");
  assert.match(full, /^    name: Test$/m);
  assert.match(full, /^    needs: validation-source$/m);
  assert.match(full, /^    if: always\(\) && !cancelled\(\) && \(github.event_name != 'pull_request' \|\| github.base_ref == 'main'\)$/m);
  assert.match(full, /- name: Require full-validation admission\n        if: needs.validation-source.result != 'success' \|\| needs.validation-source.outputs.full != 'true'\n        run: exit 1/);
  for (const command of ["cargo build --locked", "cargo test --locked --no-run", "cargo test --locked"]) assert.ok(full.includes(`        run: ${command}\n`));
  for (const id of ["msrv", "tls-fixture-test", "tool-event-recorder-e2e", "e2e-test", "lint", "build", "docs", "security", "deny"]) {
    const block = job(source, id);
    assert.match(block, /^    needs: validation-source$/m);
    assert.ok(block.includes("needs.validation-source.outputs.full == 'true'"));
    assert.doesNotMatch(block, /continue-on-error:/);
    // Do not pin a PR full gate to the head instead of the actual merge result.
    assert.doesNotMatch(block, /^          ref:/m);
  }
  assert.doesNotMatch(source, /continue-on-error:|paths-ignore:/);
  assert.match(source, /queue: .*github.event_name == 'push'.*refs\/heads\/main.*'max'/);
  assert.match(source, /cancel-in-progress: .*github.event_name != 'push'.*refs\/heads\/main/);
  assert.match(source, /group:.*inputs.snapshot_sha/);
}

test("Quick Checks is distinct; only explicit snapshots/main get honest full jobs", () => {
  assertRouting(ci);
  for (const [from, to] of [
    ["name: Quick Checks", "name: Test"],
    ["run: cargo fmt --all -- --check", "run: cargo test --locked"],
    ["if: always() && !cancelled() && (github.event_name != 'pull_request' || github.base_ref == 'main')", "if: needs.validation-source.outputs.full == 'true'"],
    ["        run: exit 1", "        run: exit 0"],
    ["if: needs.validation-source.outputs.full == 'true'", "if: true"],
    ["        run: cargo test --locked\n", "        continue-on-error: true\n        run: cargo test --locked\n"],
  ]) assert.throws(() => assertRouting(ci.replace(from, to)), `reject ${to}`);
});

test("snapshot controller executes positive and fail-closed routing cases", (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "bamboo-ci-routing-"));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const shell = script(job(ci, "validation-source"));
  assert.doesNotMatch(shell, /\$\{\{|eval|git checkout|git fetch/);
  const sha = "a".repeat(40);
  const run = (overrides) => {
    const output = path.join(directory, "output"); fs.writeFileSync(output, "");
    const result = spawnSync("bash", ["-c", shell], { encoding: "utf8", env: { ...process.env,
      GITHUB_EVENT_NAME: "workflow_dispatch", GITHUB_SHA: sha, EXPECTED_SNAPSHOT_SHA: sha,
      GITHUB_REF: "refs/heads/dev", GITHUB_BASE_REF: "", GITHUB_OUTPUT: output, ...overrides } });
    return { ...result, output: fs.readFileSync(output, "utf8") };
  };
  for (const overrides of [ {}, { GITHUB_EVENT_NAME: "pull_request", GITHUB_BASE_REF: "main" }, { GITHUB_EVENT_NAME: "push", GITHUB_REF: "refs/heads/main" } ]) {
    const result = run(overrides); assert.equal(result.status, 0, result.stderr); assert.equal(result.output, "full=true\n");
  }
  const dev = run({ GITHUB_EVENT_NAME: "pull_request", GITHUB_BASE_REF: "dev" });
  assert.equal(dev.status, 0); assert.equal(dev.output, "full=false\n");
  for (const overrides of [
    { EXPECTED_SNAPSHOT_SHA: "" }, { EXPECTED_SNAPSHOT_SHA: "dev" }, { EXPECTED_SNAPSHOT_SHA: "b".repeat(40) },
    { EXPECTED_SNAPSHOT_SHA: "A".repeat(40) }, { EXPECTED_SNAPSHOT_SHA: `${sha}\nfull=true` },
    { EXPECTED_SNAPSHOT_SHA: "$(touch injected)" }, { GITHUB_SHA: "" },
    { GITHUB_EVENT_NAME: "push" }, { GITHUB_EVENT_NAME: "pull_request", GITHUB_BASE_REF: "integration/batch" },
    { GITHUB_EVENT_NAME: "pull_request_target" }, { GITHUB_EVENT_NAME: "unknown" },
  ]) { const result = run(overrides); assert.notEqual(result.status, 0); assert.equal(result.output, ""); }
});

test("full Test binds immutable merge/event source and fails on dirty source", () => {
  const full = job(ci, "test");
  assert.doesNotMatch(full, /^          ref:/m);
  assert.equal((full.match(/test "\$\(git rev-parse HEAD\)" = "\$GITHUB_SHA"/g) || []).length, 2);
  assert.equal((full.match(/git status --porcelain --untracked-files=all/g) || []).length, 2);
  assert.match(full, /name: Require the exact clean source after full validation\n        if: always\(\)/);
  assert.match(full, /sha256sum Cargo.lock/);
  assert.match(full, /rustc --version --verbose\n          cargo --version --verbose/);
});

test("frontend source acceptance leaves ordinary dev admission but retains exact-head full commands", () => {
  assert.match(frontend, /pull_request:\n    branches: \[main\]/);
  assert.doesNotMatch(frontend, /branches: \[dev\]|paths:/);
  assert.match(frontend, /snapshot_sha:\n.*\n        required: true\n        type: string/);
  for (const id of ["lint", "test"]) {
    const block = job(frontend, id);
    assert.match(block, /ref: \$\{\{ github.event_name == 'pull_request' && github.event.pull_request.head.sha \|\| github.sha \}\}/);
    assert.match(block, /test "\$EXPECTED_SNAPSHOT_SHA" = "\$GITHUB_SHA"/);
    assert.match(block, /source_status="\$\(git status --porcelain --untracked-files=all\)"\n          test -z "\$source_status"/);
    assert.doesNotMatch(block, /continue-on-error:/);
  }
  for (const command of ["cargo build --locked", "cargo test --locked --no-run", "cargo test --locked"]) assert.ok(job(frontend, "test").includes(`        run: ${command}\n`));
  assert.match(job(frontend, "lint"), /cargo clippy --all-features --/);
});

test("publication still requires the actual successful same-repository main push CI", () => {
  assert.match(publisher, /workflow_run:\n    workflows: \[CI\]/);
  for (const condition of ["github.event.workflow_run.event == 'push'", "github.event.workflow_run.conclusion == 'success'", "github.event.workflow_run.head_branch == 'main'", "github.event.workflow_run.head_repository.full_name == github.repository"]) assert.ok(publisher.includes(condition));
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
  fs.writeFileSync(path.join(bin, "git"), '#!/bin/bash\nif [[ "$1" == status && "$FAIL_STATUS" == 1 ]]; then echo "fatal: inspection failed" >&2; exit 23; fi\nexec "$REAL_GIT" "$@"\n', { mode: 0o755 });
  const guards = [];
  for (const source of [job(ci, "dev-build"), job(ci, "test"), job(frontend, "lint"), job(frontend, "test")]) {
    for (const step of source.split(/(?=^      - )/m)) {
      if (step.includes("source_status=")) guards.push(script(step));
    }
  }
  assert.equal(guards.length, 6);
  for (const shell of guards) {
    const run = (overrides = {}) => spawnSync("bash", ["-c", shell], { cwd: repo, encoding: "utf8", env: { ...process.env,
      PATH: `${bin}${path.delimiter}${process.env.PATH}`, REAL_GIT: realGit, FAIL_STATUS: "0",
      GITHUB_SHA: sha, EXPECTED_SHA: sha, GITHUB_EVENT_NAME: "workflow_dispatch", EXPECTED_SNAPSHOT_SHA: sha, ...overrides } });
    assert.equal(run().status, 0);
    assert.notEqual(run({ FAIL_STATUS: "1" }).status, 0, "Git status failure must not be clean success");
    assert.notEqual(run({ GITHUB_SHA: "b".repeat(40) }).status, 0);
    fs.writeFileSync(path.join(repo, "untracked"), "dirty\n"); assert.notEqual(run().status, 0); fs.unlinkSync(path.join(repo, "untracked"));
    fs.writeFileSync(path.join(repo, "Cargo.lock"), "# changed\n"); assert.notEqual(run().status, 0);
    git("add", "Cargo.lock"); assert.notEqual(run().status, 0); git("reset", "--hard", sha);
  }
});


function assertDevBuild(source) {
  const build = job(source, "dev-build");
  const [settings, ...steps] = build.split(/(?=^      - )/m);
  assert.match(settings, /^    name: Dev Build$/m);
  assert.match(settings, /^    if: github.event_name == 'pull_request' && github.base_ref == 'dev'$/m);
  assert.match(settings, /^    runs-on: ubuntu-latest$/m);
  assert.match(settings, /^    timeout-minutes: 45$/m);
  assert.doesNotMatch(settings, /^    (?:needs|continue-on-error):/m);
  const cargo = steps.filter((step) => step.startsWith("      - name: Build locked Rust workspace\n"));
  assert.equal(cargo.length, 1);
  assert.equal(cargo[0].trimEnd(), "      - name: Build locked Rust workspace\n        run: cargo build --locked");
  assert.doesNotMatch(build, /cargo (?:test|check|clippy)|continue-on-error:|\|\| true|\|\| exit 0/);
  assert.doesNotMatch(build, /^          ref:/m, "Dev Build must use the actual event merge source");
  assert.match(build, /uses: actions\/checkout@v7/);
  assert.match(build, /uses: actions\/setup-node@v7/);
  assert.match(build, /uses: dtolnay\/rust-toolchain@stable/);
  assert.equal((build.match(/test "\$\(git rev-parse HEAD\)" = "\$GITHUB_SHA"/g) || []).length, 2);
  assert.equal((build.match(/source_status="\$\(git status --porcelain --untracked-files=all\)"/g) || []).length, 2);
  assert.match(build, /name: Require the exact clean source after dev build\n        if: always\(\)/);
  assert.match(build, /sha256sum Cargo.lock/);
}

test("Dev Build honestly compiles the dev merge without running the full CI test suite", () => {
  assertDevBuild(ci);
  for (const [from, to] of [
    ["name: Dev Build", "name: Quick Checks"],
    ["run: cargo build --locked", "run: cargo check --locked"],
    ["run: cargo build --locked", "run: cargo test --locked"],
    ["run: cargo build --locked", "run: cargo build --locked || true"],
    ["run: cargo build --locked", "continue-on-error: true\n        run: cargo build --locked"],
    ["run: cargo build --locked", "if: false\n        run: cargo build --locked"],
    ["    name: Dev Build", "    needs: quick-checks\n    name: Dev Build"],
  ]) assert.throws(() => assertDevBuild(ci.replace(from, to)), `reject ${to}`);
  // Existing executable controller tests prove dev emits full=false; these
  // contracts ensure that no default-workspace full test is added to dev jobs.
  assert.doesNotMatch(job(ci, "quick-checks"), /cargo (?:build|test)/);
  assert.match(job(ci, "test"), /github.event_name != 'pull_request' \|\| github.base_ref == 'main'/);
  assert.match(job(ci, "e2e-test"), /if: needs.validation-source.outputs.full == 'true'/);
});
