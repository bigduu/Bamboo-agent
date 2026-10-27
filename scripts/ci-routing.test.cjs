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
const qualityFields = ["LINT_RESULT", "MSRV_RESULT", "SECURITY_RESULT", "DENY_RESULT", "DOCS_RESULT", "CODEQL_RESULT", "FRONTEND_INVENTORY_RESULT"];
const qualitySuccess = Object.fromEntries(qualityFields.map(name => [name, "success"]));
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
  assert.match(source, /^permissions:\n  contents: read\n/m);
  assertSharedBuild(source);
  assertMainAdmission(source);
  const quick = job(source, "quick-checks");
  assert.match(quick, /^    name: Quick Checks$/m);
  assert.match(quick, /^    if: github.event_name == 'pull_request' && github.base_ref == 'dev'$/m);
  assert.match(quick, /run: cargo fmt --all -- --check/);
  assert.match(quick, /run: node --test scripts\/ci-policy.test.cjs scripts\/crate-release.test.cjs scripts\/ci-routing.test.cjs/);
  assert.doesNotMatch(quick, /cargo (build|test|check|clippy)|continue-on-error|--test-skip/);
  const full = job(source, "test");
  assert.match(full, /^    name: Test$/m);
  assert.match(full, /^    needs: \[validation-source, main-release-admission, quick-checks, feature-tests, native-feature-tests, browser-runtime-tests, lint, msrv, security, deny, docs, feature-codeql, frontend-inventory, test-crates, e2e-crates, test-ssh, tls-fixture-test, tool-event-recorder-e2e\]$/m);
  assert.match(full, /^    if: always\(\) && !cancelled\(\)$/m);
  assert.match(full, /- name: Require validation admission\n        env:\n          SOURCE_RESULT: \$\{\{ needs.validation-source.result \}\}\n          FULL_VALIDATION: \$\{\{ needs.validation-source.outputs.full \}\}/);
  assert.match(full, /test "\$SOURCE_RESULT" = success/);
  assert.match(full, /\*\) exit 1 ;;/);
  assert.doesNotMatch(full, /cargo build/);
  assert.match(full, /test "\$E2E_RESULT" = success/);
  assert.match(full, /test "\$CRATE_RESULT" = success/);
  assert.doesNotMatch(full, /run: cargo test/);
  for (const id of ["msrv", "tls-fixture-test", "tool-event-recorder-e2e", "lint", "build", "docs", "security", "deny"]) {
    const block = job(source, id);
    assert.match(block, /^    needs: validation-source$/m);
    assert.ok(block.includes("needs.validation-source.outputs.full == 'true'"));
    assert.doesNotMatch(block, /continue-on-error:/);
    // Do not pin a PR full gate to the head instead of the actual merge result.
    assert.doesNotMatch(block, /^          ref:/m);
  }
  assert.match(job(source, "build"), /^    name: Platform Tests \(\$\{\{ matrix\.os \}\}\)$/m);
  assert.doesNotMatch(source, /cargo build[^\n]*--release/);
  assert.doesNotMatch(source, /continue-on-error:|paths-ignore:/);
  assert.match(source, /queue: .*github.event_name == 'push'.*refs\/heads\/main.*'max'/);
  assert.match(source, /cancel-in-progress: .*github.event_name != 'push'.*refs\/heads\/main/);
  assert.match(source, /group:.*inputs.snapshot_sha/);
}

test("required Test runs feature tests, validates snapshots fully and authenticates main admission", () => {
  assertRouting(ci);
  for (const [from, to] of [
    ["name: Quick Checks", "name: Test"],
    ["name: Platform Tests (${{ matrix.os }})", "name: Build (${{ matrix.os }})"],
    ["run: cargo test --locked --no-fail-fast -p bamboo-skills progressive::source_tests -- --test-threads=1", "run: cargo build --release --verbose"],
    ["run: cargo fmt --all -- --check", "run: cargo test --locked --no-fail-fast"],
    ["if: always() && !cancelled()", "if: needs.validation-source.outputs.full == 'true'"],
    ['test "$SOURCE_RESULT" = success', 'true # ignored source result'],
    ["if: needs.validation-source.outputs.full == 'true'", "if: true"],
    ['test "$E2E_RESULT" = success', 'true # ignored E2E result'],
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
  for (const overrides of [ {}, { GITHUB_EVENT_NAME: "pull_request", GITHUB_BASE_REF: "main" } ]) {
    const result = run(overrides); assert.equal(result.status, 0, result.stderr); assert.equal(result.output, "full=true\nfeature=false\n");
  }
  const main = run({ GITHUB_EVENT_NAME: "push", GITHUB_REF: "refs/heads/main" });
  assert.equal(main.status, 0); assert.equal(main.output, "full=false\nfeature=false\n");
  const dev = run({ GITHUB_EVENT_NAME: "pull_request", GITHUB_BASE_REF: "dev" });
  assert.equal(dev.status, 0); assert.equal(dev.output, "full=false\nfeature=true\n");
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

test("frontend source acceptance retains the exact-head build and focused frontend tests", () => {
  assert.match(frontend, /pull_request:\n    branches: \[main\]/);
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

test("publication still requires the actual successful same-repository main push CI", () => {
  assert.match(publisher, /workflow_run:\n    workflows: \[CI\]/);
  for (const condition of ["github.event.workflow_run.event == 'push'", "github.event.workflow_run.conclusion == 'success'", "github.event.workflow_run.head_branch == 'main'", "github.event.workflow_run.head_repository.full_name == github.repository"]) assert.ok(publisher.includes(condition));
});

function assertMainAdmission(source) {
  const admission = job(source, "main-release-admission");
  assert.match(admission, /name: Main Release Admission/);
  assert.match(admission, /needs: validation-source/);
  assert.match(admission, /if: always\(\) && !cancelled\(\) && github.event_name == 'push' && github.ref == 'refs\/heads\/main'/);
  assert.match(admission, /permissions:\n      contents: read\n      actions: read/);
  assert.match(admission, /ADMISSION_MODE: main/);
  assert.doesNotMatch(admission, /actions\/checkout|contents: write|cargo |secrets\./);
  const full = job(source, "test");
  assert.match(full, /MAIN_ADMISSION_RESULT: \$\{\{ needs.main-release-admission.result \}\}/);
  assert.match(full, /test "\$MAIN_ADMISSION_RESULT" = success/);
  const [, admissionStep, ...sourceSteps] = full.split(/(?=^      - )/m);
  assert.match(admissionStep, /name: Require validation admission/);
  for (const step of sourceSteps) {
    assert.match(step, /^        if: (?:(?:always|failure)\(\) && )?needs.validation-source.outputs.full == 'true'$/m,
      "main must perform admission only, without checkout, compilation or duplicate tests");
  }
}

test("main Test reports real admission while the full suite remains on pre-merge/manual sources", () => {
  assertMainAdmission(ci);
  for (const [from, to] of [
    ["ADMISSION_MODE: main", "ADMISSION_MODE: publication"],
    ['test "$MAIN_ADMISSION_RESULT" = success', 'true'],
    ["if: needs.validation-source.outputs.full == 'true'", "if: always()"],
    ["contents: read\n      actions: read", "contents: write\n      actions: read"],
  ]) assert.throws(() => assertMainAdmission(ci.replace(from, to)));
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
  for (const source of [job(ci, "test"), job(frontend, "lint"), job(frontend, "build"), job(frontend, "test"),
    fs.readFileSync(".github/workflows/root-birth-acceptance.yml", "utf8"),
    ...["lint", "msrv", "security", "deny", "docs", "feature-codeql", "frontend-inventory"].map(id => job(ci, id))]) {
    for (const step of source.split(/(?=^      - )/m)) {
      if (step.includes("source_status=") || step.includes("status=$(git status")) guards.push(script(step));
    }
  }
  assert.equal(guards.length, 23);
  for (const shell of guards) {
    const run = (overrides = {}) => spawnSync("bash", ["-c", shell], { cwd: repo, encoding: "utf8", env: { ...process.env,
      PATH: `${bin}${path.delimiter}${process.env.PATH}`, REAL_GIT: realGit, FAIL_STATUS: "0",
      GITHUB_SHA: sha, EXPECTED_SHA: sha, EXPECTED_SOURCE: sha, EXPECTED_TREE: tree,
      BUILD_SOURCE_SHA: sha, BUILD_SOURCE_TREE: tree, GITHUB_OUTPUT: path.join(directory, "outputs"),
      GITHUB_EVENT_NAME: "workflow_dispatch", EXPECTED_SNAPSHOT_SHA: sha, ...overrides } });
    const clean = run(); assert.equal(clean.status, 0, clean.stderr);
    assert.notEqual(run({ FAIL_STATUS: "1" }).status, 0, "Git status failure must not be clean success");
    assert.notEqual(run({ GITHUB_SHA: "b".repeat(40), EXPECTED_SHA: "b".repeat(40), EXPECTED_SOURCE: "b".repeat(40) }).status, 0);
    fs.writeFileSync(path.join(repo, "untracked"), "dirty\n"); assert.notEqual(run().status, 0); fs.unlinkSync(path.join(repo, "untracked"));
    fs.writeFileSync(path.join(repo, "Cargo.lock"), "# changed\n"); assert.notEqual(run().status, 0);
    git("add", "Cargo.lock"); assert.notEqual(run().status, 0); git("reset", "--hard", sha);
  }
});


function assertSharedBuild(source) {
  assert.doesNotMatch(source, /^  dev-build:|^    name: Dev Build$/m);
  assert.equal((source.match(/^    name: Test$/gm) || []).length, 1);
  const build = job(source, "test");
  const [settings, ...steps] = build.split(/(?=^      - )/m);
  assert.match(settings, /^    name: Test$/m);
  assert.match(settings, /^    if: always\(\) && !cancelled\(\)$/m);
  assert.match(settings, /^    needs: \[validation-source, main-release-admission, quick-checks, feature-tests, native-feature-tests, browser-runtime-tests, lint, msrv, security, deny, docs, feature-codeql, frontend-inventory, test-crates, e2e-crates, test-ssh, tls-fixture-test, tool-event-recorder-e2e\]$/m);
  assert.ok(settings.includes("timeout-minutes: ${{ github.event_name == 'pull_request' && github.base_ref == 'dev' && 45 || 75 }}"));
  assert.doesNotMatch(settings, /^    continue-on-error:/m);
  assert.doesNotMatch(build, /cargo build/);
  assert.match(build, /test "\$E2E_RESULT" = success/);
  assert.doesNotMatch(build, /continue-on-error:|\|\| true|\|\| exit 0/);
  assert.doesNotMatch(build, /^          ref:/m, "retain the actual event merge source");
  for (const name of ["Test CI workflow policy", "Check formatting", "Require all crate tests and the SSH fixture"]) {
    const step = steps.find((item) => item.startsWith(`      - name: ${name}\n`));
    assert.ok(step);
    assert.ok(step.includes("if: needs.validation-source.outputs.full == 'true'"));

  }
  const admission = steps.find((step) => step.startsWith("      - name: Require validation admission\n"));
  assert.equal(steps.indexOf(admission), 0);
  assert.doesNotMatch(admission, /^        (?:if|continue-on-error):/m);
}

test("one required Test requires successful Unit, E2E and SSH jobs", () => {
  assertSharedBuild(ci);
  for (const [from, to] of [
    ["name: Test\n", "name: Dev Build\n"],
    ['test "$E2E_RESULT" = success', 'true'],
    ["needs: [validation-source, main-release-admission, quick-checks, feature-tests, native-feature-tests, browser-runtime-tests, lint, msrv, security, deny, docs, feature-codeql, frontend-inventory, test-crates, e2e-crates, test-ssh, tls-fixture-test, tool-event-recorder-e2e]", "needs: quick-checks"],
    ["name: Test CI workflow policy\n        if: needs.validation-source.outputs.full == 'true'", "name: Test CI workflow policy"],
    ["name: Check formatting\n        if: needs.validation-source.outputs.full == 'true'", "name: Check formatting"],
    ["name: Require all crate tests and the SSH fixture\n        if: needs.validation-source.outputs.full == 'true'", "name: Require all crate tests and the SSH fixture"],
  ]) assert.throws(() => assertSharedBuild(ci.replace(from, to)), `reject ${to}`);
});

test("required Test admits only successful feature execution or the matching legacy release/main proof", () => {
  const shell = script(job(ci, "test").split("      - uses:")[0]);
  const run = (overrides = {}) => spawnSync("bash", ["-c", shell], { encoding: "utf8", env: { ...process.env,
    ...qualitySuccess, SOURCE_RESULT: "success", FULL_VALIDATION: "false", FEATURE_VALIDATION: "true", FEATURE_RESULT: "success",
    QUICK_RESULT: "success", NATIVE_RESULT: "success", BROWSER_RUNTIME_RESULT: "success", MAIN_ADMISSION_RESULT: "success",
    GITHUB_EVENT_NAME: "pull_request", GITHUB_BASE_REF: "dev", GITHUB_REF: "refs/pull/1/merge", ...overrides } });
  for (const overrides of [{}, { GITHUB_BASE_REF: "main", FULL_VALIDATION: "true", FEATURE_VALIDATION: "false", CODEQL_RESULT: "skipped", FRONTEND_INVENTORY_RESULT: "skipped" },
    { GITHUB_EVENT_NAME: "workflow_dispatch", GITHUB_BASE_REF: "", FULL_VALIDATION: "true", FEATURE_VALIDATION: "false" },
    { GITHUB_EVENT_NAME: "push", GITHUB_BASE_REF: "", GITHUB_REF: "refs/heads/main", FEATURE_VALIDATION: "false", ...Object.fromEntries(qualityFields.map(name => [name, "skipped"])) }]) assert.equal(run(overrides).status, 0);
  for (const field of ["SOURCE_RESULT", "FEATURE_RESULT", "QUICK_RESULT", "NATIVE_RESULT", "BROWSER_RUNTIME_RESULT", ...qualityFields]) {
    for (const value of ["failure", "cancelled", "skipped", "", "unknown", undefined]) assert.notEqual(run({ [field]: value }).status, 0);
  }
  for (const overrides of [
    { FULL_VALIDATION: "true" }, { FULL_VALIDATION: "" }, { FEATURE_VALIDATION: "false" },
    { GITHUB_BASE_REF: "main" }, { GITHUB_BASE_REF: "integration/batch" },
    { GITHUB_EVENT_NAME: "workflow_dispatch", GITHUB_BASE_REF: "" },
    { GITHUB_EVENT_NAME: "push", GITHUB_BASE_REF: "", GITHUB_REF: "refs/heads/dev" },
    { GITHUB_EVENT_NAME: "push", GITHUB_BASE_REF: "", GITHUB_REF: "refs/heads/main" },
    ...["failure", "cancelled", "skipped", "", "unknown"].map(MAIN_ADMISSION_RESULT => ({ GITHUB_EVENT_NAME: "push", GITHUB_BASE_REF: "", GITHUB_REF: "refs/heads/main", FEATURE_VALIDATION: "false", MAIN_ADMISSION_RESULT })),
    { GITHUB_EVENT_NAME: "pull_request_target" }, { GITHUB_EVENT_NAME: "unknown" },
  ]) assert.notEqual(run(overrides).status, 0);
});

test("both summaries reject every incomplete quality owner and preserve legacy/main routing", () => {
  const shell = script(job(ci, "e2e-test"));
  const run = (overrides = {}) => spawnSync("bash", ["-c", shell], { encoding: "utf8", env: { ...process.env,
    ...qualitySuccess, SOURCE_RESULT: "success", FULL_VALIDATION: "false", FEATURE_VALIDATION: "true", FEATURE_RESULT: "success", NATIVE_RESULT: "success", BROWSER_RUNTIME_RESULT: "success", CRATE_RESULT: "success", SSH_RESULT: "success", ...overrides } });
  assert.equal(run().status, 0);
  assert.equal(run({ FULL_VALIDATION: "true", FEATURE_VALIDATION: "false", FEATURE_RESULT: "skipped", NATIVE_RESULT: "skipped", BROWSER_RUNTIME_RESULT: "skipped", CODEQL_RESULT: "skipped", FRONTEND_INVENTORY_RESULT: "skipped" }).status, 0);
  for (const field of ["SOURCE_RESULT", "FEATURE_RESULT", "NATIVE_RESULT", "BROWSER_RUNTIME_RESULT", ...qualityFields]) for (const value of ["failure", "cancelled", "skipped", "", "unknown", undefined]) {
    assert.notEqual(run({ [field]: value }).status, 0, field + ":" + value);
  }
  for (const field of ["CRATE_RESULT", "SSH_RESULT"]) assert.notEqual(run({ FULL_VALIDATION: "true", FEATURE_VALIDATION: "false", [field]: "skipped" }).status, 0);
  for (const overrides of [{ FEATURE_VALIDATION: "" }, { FULL_VALIDATION: "true" }, { FEATURE_VALIDATION: "false" }]) assert.notEqual(run(overrides).status, 0);
  const testShell = script(job(ci, "test").split("      - uses:")[0]);
  for (const field of qualityFields.slice(0, 5)) for (const value of ["failure", "cancelled", "skipped", "", undefined]) {
    assert.notEqual(run({ FULL_VALIDATION: "true", FEATURE_VALIDATION: "false", [field]: value }).status, 0);
    assert.notEqual(spawnSync("bash", ["-c", testShell], { env: { ...process.env, ...qualitySuccess,
      SOURCE_RESULT: "success", FULL_VALIDATION: "true", FEATURE_VALIDATION: "false",
      GITHUB_EVENT_NAME: "pull_request", GITHUB_BASE_REF: "main", [field]: value } }).status, 0);
  }
  for (const id of ["test", "e2e-test"]) for (const owner of ["lint", "msrv", "security", "deny", "docs", "feature-codeql", "frontend-inventory"]) {
    assert.ok(job(ci, id).match(/^    needs: \[([^\]]+)\]$/m)[1].split(", ").includes(owner));
    assert.ok(job(ci, id).includes(`needs.${owner}.result`));
  }
});

test("feature execution shares owned outputs across explicit configurations and excludes duplicate Linux fixtures", () => {
  const feature = job(ci, "feature-tests");
  assert.match(feature, /if: needs.validation-source.outputs.feature == 'true'/);
  assert.match(feature, /CARGO_TARGET_DIR: \$\{\{ github.workspace \}\}\/target\/feature-tests/);
  assert.match(feature, /run: node scripts\/run-feature-tests.cjs/);
  assert.match(feature, /node scripts\/frontend-package.cjs verify/);
  assert.doesNotMatch(feature, /cargo build|cargo test|continue-on-error|secrets\./);
  const recorder = job(ci, "tool-event-recorder-e2e");
  assert.match(recorder, /^    if: needs.validation-source.outputs.full == 'true'$/m);
  const native = job(ci, 'native-feature-tests');
  assert.match(native, /os: \[windows-2025, macos-latest\]/);
  assert.match(native, /run: node scripts\/native-test-owner.cjs/);
  assert.match(native, /PR_HEAD_SHA: \$\{\{ github.event.pull_request.head.sha \}\}/);
  assert.doesNotMatch(native, /secrets\.|continue-on-error|cargo test/);
  assert.equal(fs.existsSync('.github/workflows/orchestration-native-recovery.yml'), false);
  assert.equal(fs.existsSync('.github/workflows/scoped-context-view.yml'), false);
  const identity = fs.readFileSync('.github/workflows/jiandu-project-source.yml', 'utf8');
  assert.match(identity, /cargo metadata --locked --all-features/);
  assert.doesNotMatch(identity, /cargo (test|clippy|build)/);
});
