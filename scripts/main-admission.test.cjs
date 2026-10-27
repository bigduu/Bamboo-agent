"use strict";
// Inert API/event fixtures and tiny Git only; no real check or producer is installed.
const assert = require("node:assert/strict"), { test } = require("node:test");
const fs = require("node:fs"), path = require("node:path"), os = require("node:os");
const { execFileSync, spawnSync } = require("node:child_process");
const { createHash } = require("node:crypto"), Module = require("node:module");
const controller = require("./main-admission.cjs"), proof = require("./published-source-admission.cjs");
const core = require("./crate-release.cjs"), source = require("./frozen-source-admission.cjs");
const { frozenSourceFixture } = require("./tests/source-admission-fixture.cjs");
const workflow = fs.readFileSync(path.join(__dirname, "..", proof.MAIN_WORKFLOW));
const safe = { PATH: "/opt/homebrew/bin:/usr/bin:/bin", HOME: os.tmpdir(), TMPDIR: os.tmpdir() };
const copy = value => structuredClone(value), h = byte => byte.repeat(40);
const blob = bytes => { bytes = Buffer.from(bytes); return { sha: createHash("sha1").update(`blob ${bytes.length}\0`).update(bytes).digest("hex"), size: bytes.length, encoding: "base64", content: bytes.toString("base64") }; };
const body = value => "<!-- bamboo-main-locator/v1\n" + JSON.stringify(value) + "\n-->";
function model(B = h("b")) {
  const F = h("f"), M = h("d"), S = h("4"), repository = { id: 1164779771, full_name: source.REPOSITORY, default_branch: "main" };
  const locator = { mode: "promotion", sourceRevision: S, featurePr: 99, ciRun: 100, releaseRun: 900 };
  const pr = { number: 88, state: "open", merged: false, draft: false, mergeable: true, body: body(locator), merge_commit_sha: M,
    base: { ref: "main", sha: B, repo: repository }, head: { ref: "promotion/test", sha: F, repo: repository } };
  const event = { repository, action: "opened", number: 88, pull_request: copy(pr) };
  const env = { ...safe, GITHUB_REPOSITORY: source.REPOSITORY, GITHUB_EVENT_NAME: "pull_request_target", GITHUB_ACTIONS: "true",
    GITHUB_JOB: "main-admission", GITHUB_REF: "refs/heads/main", GITHUB_REF_PROTECTED: "true", GITHUB_SHA: B, GITHUB_WORKFLOW_SHA: B,
    GITHUB_WORKFLOW_REF: `${source.REPOSITORY}/${proof.MAIN_WORKFLOW}@refs/heads/main`, GITHUB_RUN_ID: "700", GITHUB_RUN_ATTEMPT: "1" };
  const run = { id: 700, run_attempt: 1, workflow_id: 701, check_suite_id: 702, check_suite_node_id: "CS_inert_main",
    repository, head_repository: repository, event: "pull_request_target", path: proof.MAIN_WORKFLOW, name: "Bamboo Main Admission",
    head_sha: F, head_branch: pr.head.ref, status: "in_progress", conclusion: null };
  const suite = { databaseId: 702, repository: { databaseId: repository.id, nameWithOwner: repository.full_name }, app: { databaseId: 15368, slug: "github-actions" },
    commit: { oid: F }, workflowRun: { databaseId: 700, event: run.event, runAttempt: 1, workflow: { databaseId: 701, name: run.name },
      file: { path: proof.MAIN_WORKFLOW, repositoryName: repository.full_name, repositoryFileUrl: `https://github.com/${repository.full_name}/blob/${F}/${proof.MAIN_WORKFLOW}` } } };
  const job = { id: 703, run_id: 700, run_attempt: 1, name: proof.MAIN_JOB, head_sha: F, head_branch: pr.head.ref,
    status: "in_progress", conclusion: null, steps: [{ name: proof.MAIN_STEP, status: "in_progress", conclusion: null }] };
  const branch = { name: "main", protected: true, commit: { sha: B }, protection: { required_status_checks: {
    enforcement_level: "everyone", contexts: [proof.MAIN_JOB], checks: [{ context: proof.MAIN_JOB, app_id: 15368 }] } } };
  const state = { pr, run, suite, job, branch, repository }, calls = [];
  const reads = { json: async route => {
    calls.push(route);
    if (route === "") return copy(state.repository);
    if (route === "branches/main") return copy(state.branch);
    if (route === "pulls/88") return copy(state.pr);
    if (route === "actions/runs/700") return copy(state.run);
    if (route === "actions/workflows/main-admission.yml") return { id: 701, path: proof.MAIN_WORKFLOW, state: "active" };
    if (route.startsWith("actions/runs/700/attempts/1/jobs?")) return { total_count: 1, jobs: [copy(state.job)] };
    if (route === `contents/${proof.MAIN_WORKFLOW}?ref=${B}`) return { path: proof.MAIN_WORKFLOW, ...blob(workflow) };
    assert.fail("Unexpected inert Main read: " + route);
  }, checkSuite: async id => { assert.equal(id, "CS_inert_main"); return copy(state.suite); } };
  return { B,F,M,S,env,event,state,reads,calls,locator };
}
function tiny(t) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bamboo-main2351-"));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const git = args => execFileSync("git", ["-c", "core.hooksPath=/dev/null", "-c", "commit.gpgsign=false", ...args], { cwd: root, env: safe, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] });
  git(["init", "-q"]);
  for (const file of proof.MAIN_CONTROLS) { const target = path.join(root, file); fs.mkdirSync(path.dirname(target), { recursive: true }); fs.writeFileSync(target, file === proof.MAIN_WORKFLOW ? workflow : "inert trusted B control\n"); }
  fs.writeFileSync(path.join(root, "product.txt"), "stable product; never executed\n");
  const commit = () => { git(["add", "."]); git(["-c", "user.name=Inert Test", "-c", "user.email=inert@example.invalid", "commit", "-qm", "inert B"]); return git(["rev-parse", "HEAD"]).trim(); };
  return { root, git, commit, B: commit() };
}
test("Main2351 actual event identity keeps B separate from F and refuses default dev/bootstrap", () => {
  const f = model(), value = controller.eventIdentity(f.env, f.event); assert.equal(value.B, f.B); assert.equal(value.F, f.F);
  for (const mutate of [g => g.event.repository.default_branch = "dev", g => g.env.GITHUB_SHA = g.M,
    g => g.env.GITHUB_EVENT_NAME = "pull_request", g => g.env.GITHUB_WORKFLOW_REF = "candidate/workflow",
    g => g.event.pull_request.body = body({ mode: "bootstrap", sourceRevision: g.S, featurePr: 99, ciRun: 100 })]) {
    const g = model(); mutate(g); assert.throws(() => controller.eventIdentity(g.env, g.event));
  }
});
test("Main2351 runtime verifies real in-progress Target job with F suite/file metadata", async () => {
  const f = model(), result = await controller.runtime(f.env, f.event, f.reads);
  assert.equal(result.execution.revision, f.B); assert.equal(result.execution.metadataRevision, f.F); assert.equal(result.M, f.M);
});
for (const fault of ["stale-base", "head", "locator", "workflow", "event", "file-as-B", "foreign-app", "success-stub", "skipped-step", "protection", "fresh-default", "missing-branch"])
test("Main2351 runtime rejects " + fault, async () => {
  const f = model();
  if (fault === "stale-base") f.state.branch.commit.sha = f.S;
  if (fault === "head") f.state.pr.head.sha = f.S;
  if (fault === "locator") f.state.pr.body = body({ ...f.locator, releaseRun: 999 });
  if (fault === "workflow") f.state.run.workflow_id++;
  if (fault === "event") f.state.run.event = "pull_request";
  if (fault === "file-as-B") f.state.suite.workflowRun.file.repositoryFileUrl = f.state.suite.workflowRun.file.repositoryFileUrl.replace(f.F, f.B);
  if (fault === "foreign-app") f.state.suite.app.databaseId = 99;
  if (fault === "success-stub") Object.assign(f.state.job, { status: "completed", conclusion: "success", steps: [] });
  if (fault === "skipped-step") f.state.job.steps[0].status = "completed";
  if (fault === "protection") f.state.branch.protection.required_status_checks.enforcement_level = "off";
  if (fault === "fresh-default") f.state.repository.default_branch = "dev";
  if (fault === "missing-branch") { delete f.state.pr.head.ref; delete f.event.pull_request.head.ref; delete f.state.run.head_branch; delete f.state.job.head_branch; }
  await assert.rejects(controller.runtime(f.env, f.event, f.reads));
});
test("Main2351 tiny Git checks whole clean ordinary B closure and raw bytes", t => {
  const f = tiny(t); controller.checkout(f.root, f.B, safe);
  fs.appendFileSync(path.join(f.root, "scripts/main-admission.cjs"), "drift");
  assert.throws(() => controller.checkout(f.root, f.B, safe), /clean/);
});
test("Main2351 tiny Git rejects committed symlink and missing control", t => {
  for (const mode of ["symlink", "missing"]) {
    const f = tiny(t), target = path.join(f.root, "scripts/main-admission.cjs"); fs.unlinkSync(target);
    if (mode === "symlink") fs.symlinkSync("../product.txt", target);
    assert.throws(() => controller.checkout(f.root, f.commit(), safe));
  }
});
test("Main2351 real tiny CLI stops on default dev before any GitHub read", t => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "bamboo-main2351-stop-")); t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const f = model(); f.event.repository.default_branch = "dev";
  const eventPath = path.join(directory, "event.json"); fs.writeFileSync(eventPath, JSON.stringify(f.event));
  const result = spawnSync(process.execPath, [path.join(__dirname, "main-admission.cjs")], { cwd: directory,
    env: { ...f.env, PATH: "/nonexistent", GITHUB_EVENT_PATH: eventPath }, encoding: "utf8", timeout: 10000 });
  assert.equal(result.status, 1); assert.equal(result.stdout, ""); assert.match(result.stderr, /STOP: separate Ops must select default Main/);
});
test("Main2351 fixed read adapters isolate credentials and reject arbitrary routes", async () => {
  const calls = [], env = { ...safe, GH_TOKEN: "inert-read-fixture", CARGO_REGISTRY_TOKEN: "unused-inert-fixture", NODE_OPTIONS: "unused-inert-fixture" };
  const reads = controller.createReads(env, (command, args, options) => { calls.push({ command, args, options }); return { status: 0, stdout: "{}" }; });
  await reads.json(""); await reads.checkSuite("CS_inert"); await reads.jobLog(1);
  for (const call of calls) { assert.equal(call.command, "gh"); assert.deepEqual(Object.keys(call.options.env).sort(), ["GH_PROMPT_DISABLED", "GH_TOKEN", "HOME", "PATH", "TMPDIR"]); }
  assert.equal(calls[0].args.at(-1), "repos/" + source.REPOSITORY);
  for (const route of ["/outside", "../outside", "https://example.invalid", "contents/%2e%2e/file"]) await assert.rejects(reads.json(route));
  assert.deepEqual(Object.keys(controller.childEnv(env)).sort(), ["GH_PROMPT_DISABLED", "HOME", "PATH", "TMPDIR"]);
});
test("Main2351 registry read sends no GitHub credential and bounds its own checksum data", async () => {
  let options;
  const reads = controller.createReads(safe, () => assert.fail("No GitHub read"), async (url, value) => {
    assert.equal(url, "https://crates.io/api/v1/crates/bamboo-agent/2026.10.27"); options = value;
    return new Response(JSON.stringify({ version: { crate: "bamboo-agent", num: "2026.10.27", checksum: "a".repeat(64) } }));
  });
  assert.equal(await reads.registryChecksum("bamboo-agent", "2026.10.27"), "a".repeat(64));
  assert.deepEqual(options.headers, { Accept: "application/json" }); assert.equal(options.redirect, "error");
});
function bootstrapFixture() {
  const f = frozenSourceFixture(), S = f.input.sourceRevision, B = controller.LEGACY_BASE, F = h("a"), M = h("0"), tree = h("9");
  for (const file of proof.MAIN_CONTROLS) if (!(file in f.files[S])) f.files[S][file] = "inert accepted-D control " + file;
  f.files[S][proof.MAIN_WORKFLOW] = workflow.toString();
  const leaves = entries => new Map(Object.entries(entries).map(([file, bytes]) => [file, { mode: "100644", sha: blob(bytes).sha }]));
  const product = leaves({ "src/lib.rs": "stable product", "Cargo.lock": "stable lock", "scripts/frontend-package-lock.json": "stable frontend lock" });
  product.set("vendor/module", { mode: "160000", sha: h("7") });
  const controls = leaves(f.files[S]), overlay = new Map(product); for (const file of proof.MAIN_CONTROLS) overlay.set(file, controls.get(file));
  const rows = (sha, map) => ({ sha, truncated: false, tree: [...map].map(([path, value]) => ({ path, type: value.mode === "160000" ? "commit" : "blob", ...value })) });
  const routes = new Map([["git/commits/" + B, { sha: B, tree: { sha: controller.LEGACY_TREE } }],
    ["git/commits/" + F, { sha: F, tree: { sha: tree }, parents: [{ sha: B }] }],
    ["git/commits/" + M, { sha: M, tree: { sha: tree }, parents: [{ sha: B }, { sha: F }] }],
    ["git/trees/" + controller.LEGACY_TREE + "?recursive=1", rows(controller.LEGACY_TREE, product)],
    ["git/trees/" + f.accepted.tree.sha + "?recursive=1", rows(f.accepted.tree.sha, controls)],
    ["git/trees/" + tree + "?recursive=1", rows(tree, overlay)]]);
  const locator = { mode: "bootstrap", sourceRevision: S, featurePr: f.input.featurePr, ciRun: f.input.ciRun };
  const pr = { number: 88, state: "open", merged: false, draft: false, mergeable: true, body: body(locator), merge_commit_sha: M,
    base: { ref: "main", sha: B, repo: f.run.repository }, head: { ref: "bootstrap/test", sha: F, repo: f.run.repository } };
  const original = f.reads.json, reads = { ...f.reads, json: async route => {
    if (routes.has(route)) return copy(routes.get(route));
    if (route === "pulls/88") return copy(pr);
    if (route === "branches/main") return { name: "main", protected: false, commit: { sha: B } };
    return original(route);
  } };
  return { f, pr, routes, tree, reads, input: { sourceRevision: S, featurePr: f.input.featurePr, ciRun: f.input.ciRun, pullRequest: 88, headRevision: F, snapshotRevision: M } };
}
test("Main2351 Ops bootstrap reads actual accepted-D facts and preserves every legacy product/lock/gitlink", async () => {
  const f = bootstrapFixture(), result = await controller.verifyBootstrap(f.input, f.reads, f.f.policy);
  assert.equal(result.publicationAuthorized, false); assert.equal(result.legacyProductTree, controller.LEGACY_TREE);
  assert.equal(result.feature.snapshotRevision, f.f.snapshot.sha); assert.ok(f.f.calls.includes("pulls/2333"));
  await assert.rejects(controller.verifyBootstrap(f.input, f.reads), /inactive/);
});
test("Main2351 Ops bootstrap ignores unrelated repository counters and rejects caller control policy", async () => {
  const f = bootstrapFixture(), original = f.reads.json; let count = 0;
  f.reads.json = async route => { const value = await original(route); if (route === "pulls/88") value.head.repo.stargazers_count = ++count; return value; };
  assert.equal((await controller.verifyBootstrap(f.input, f.reads, f.f.policy)).publicationAuthorized, false);
  await assert.rejects(controller.verifyBootstrap(f.input, f.reads, { ...f.f.policy, controlPaths: [] }), /Caller-selected/);
});
for (const fault of ["product", "lock", "gitlink", "partial", "symlink", "parents", "installed", "default-inactive"])
test("Main2351 Ops bootstrap rejects " + fault, async () => {
  const f = bootstrapFixture(), rows = f.routes.get("git/trees/" + f.tree + "?recursive=1").tree;
  if (fault === "product") rows.find(row => row.path === "src/lib.rs").sha = h("1");
  if (fault === "lock") rows.find(row => row.path === "Cargo.lock").sha = h("1");
  if (fault === "gitlink") rows.find(row => row.path === "vendor/module").sha = h("1");
  if (fault === "partial") rows.find(row => row.path === proof.MAIN_CONTROLS[0]).sha = h("1");
  if (fault === "symlink") f.routes.get("git/trees/" + f.f.accepted.tree.sha + "?recursive=1").tree.find(row => row.path === proof.MAIN_CONTROLS[0]).mode = "120000";
  if (fault === "parents") f.routes.get("git/commits/" + f.input.snapshotRevision).parents.reverse();
  if (fault === "installed") f.routes.get("git/trees/" + controller.LEGACY_TREE + "?recursive=1").tree.push({ path: proof.MAIN_WORKFLOW, mode: "100644", type: "blob", sha: h("1") });
  if (fault === "default-inactive") f.f.policy = {};
  await assert.rejects(controller.verifyBootstrap(f.input, f.reads, f.f.policy));
});
test("Main2351 wrapper emits one frame only after successful proof and fresh runtime/clean B", async t => {
  const git = tiny(t), f = model(git.B), filename = path.join(__dirname, "main-admission.cjs"); let calls = 0;
  const m = new Module(filename, module); m.filename = filename;
  const normal = Module.createRequire(filename);
  m.require = name => name === "./published-source-admission.cjs" ? { ...proof, verifyPromotion: async input => {
    calls++; assert.equal(input.baseRevision, git.B); assert.equal(input.snapshotRevision, f.M);
    return { source: { revision: f.S, tree: h("e") }, completionSha256: "a".repeat(64), promotion: {
      pullRequest: 88, baseRevision: git.B, headRevision: f.F, snapshotRevision: f.M, snapshotTree: h("9"), controlMode: "complete-trusted-base", controlsSha256: "c".repeat(64) } };
  } } : normal(name);
  m._compile(fs.readFileSync(filename, "utf8"), filename);
  const line = await m.exports.executeMain(f.env, f.event, git.root, f.reads);
  const record = proof.logFrame("2026-10-27T00:00:00Z " + line); assert.equal(calls, 1); assert.equal(record.execution.revision, git.B);
  assert.equal(record.execution.metadataRevision, f.F); assert.equal(record.promotion.snapshotRevision, f.M);
  assert.equal("mergeRevision" in record.promotion, false, "Pre-merge frame cannot invent P");
  f.state.pr.head.sha = f.S;
  await assert.rejects(m.exports.executeMain(f.env, f.event, git.root, f.reads)); assert.equal(calls, 1);
});
