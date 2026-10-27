#!/usr/bin/env node
"use strict";
// Only trusted Main C runs this consumer. Site archives and receipts are data.
const assert = require("node:assert/strict"), fs = require("node:fs"), path = require("node:path");
const { spawnSync } = require("node:child_process");
const admission = require("./main-admission.cjs"), proof = require("./published-source-admission.cjs");
const source = require("./frozen-source-admission.cjs"), core = require("./crate-release.cjs");
const WORKFLOW = ".github/workflows/docs.yml", NAME = "Documentation Deployment";
const CONTROLS = [WORKFLOW, "scripts/docs-deploy.cjs", "scripts/docs-deploy-verify.py"];
const LIMIT = 1024 ** 3, UPLOAD = "Retain original feature documentation tar and receipt";
const STEPS = ["Bind feature documentation to the actual clean merge source", "Build feature documentation once",
  "Freeze original feature documentation tar and receipt", "Verify feature documentation source stayed clean", UPLOAD];
const COMMANDS = { inspect: ["Identify Main documentation disposition", "Inspect Main event"],
  prepare: ["Deploy to GitHub Pages", "Verify retained documentation originals"],
  guard: ["Deploy to GitHub Pages", "Recheck Main before Pages deployment"] };
const SHA = /^[0-9a-f]{40}$/, DIGEST = /^[0-9a-f]{64}$/;
const positive = value => { assert.ok(Number.isSafeInteger(value) && value > 0); return value; };
const numeric = value => { assert.match(String(value), /^[1-9][0-9]*$/); return positive(Number(value)); };
const hash = value => core.sha256(value), clone = value => JSON.parse(JSON.stringify(value));
const repo = value => { assert.equal(value?.id, 1164779771); assert.equal(value.full_name, source.REPOSITORY); };
function fileBytes(file, maximum) {
  const before = fs.lstatSync(file);
  assert.ok(before.isFile() && !before.isSymbolicLink() && before.nlink === 1 && before.size > 0 && before.size <= maximum);
  const fd = fs.openSync(file, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW || 0));
  try {
    const opened = fs.fstatSync(fd); assert.equal(opened.ino, before.ino); assert.equal(opened.dev, before.dev);
    const bytes = fs.readFileSync(fd), after = fs.fstatSync(fd);
    assert.equal(bytes.length, before.size); assert.equal(after.size, before.size); assert.equal(after.mtimeMs, opened.mtimeMs);
    return bytes;
  } finally { fs.closeSync(fd); }
}
const jsonFile = (file, maximum) => JSON.parse(fileBytes(file, maximum));
function directory(ctx, create = false) {
  const root = fs.realpathSync(ctx.env.RUNNER_TEMP), output = path.join(root, `bamboo-docs-${ctx.runId}-${ctx.attempt}`);
  if (create) fs.mkdirSync(output, { mode: 0o700 });
  assert.ok(fs.lstatSync(output).isDirectory() && !fs.lstatSync(output).isSymbolicLink() && fs.realpathSync(output) === output);
  return output;
}
function output(env, values) {
  fs.appendFileSync(env.GITHUB_OUTPUT, Object.entries(values).map(([name, value]) => {
    assert.ok(!String(value).includes("\n") && !String(value).includes("\r")); return `${name}=${value}\n`;
  }).join(""));
}
async function createContext() {
  assert.equal(arguments.length, 0, "No caller proof, policy, environment or authority override");
  const env = process.env, event = jsonFile(env.GITHUB_EVENT_PATH, 1024 * 1024), command = process.argv[2];
  assert.ok(Object.hasOwn(COMMANDS, command)); repo(event.repository);
  assert.equal(event.repository.default_branch, "main", "STOP: separate Ops must select default Main");
  assert.equal(env.GITHUB_ACTIONS, "true"); assert.equal(env.GITHUB_REPOSITORY, source.REPOSITORY);
  assert.equal(env.GITHUB_EVENT_NAME, "push"); assert.equal(env.GITHUB_REF, "refs/heads/main");
  assert.equal(env.GITHUB_REF_PROTECTED, "true"); assert.equal(event.ref, env.GITHUB_REF);
  assert.equal(event.forced, false); assert.equal(event.deleted, false);
  const C = env.GITHUB_WORKFLOW_SHA; assert.match(C, SHA); assert.equal(env.GITHUB_SHA, C); assert.equal(event.after, C);
  assert.equal(env.GITHUB_WORKFLOW_REF, `${source.REPOSITORY}/${WORKFLOW}@refs/heads/main`);
  assert.equal(env.GITHUB_JOB, command === "inspect" ? "inspect-docs" : "deploy-docs");
  for (const file of CONTROLS) assert.ok(proof.MAIN_CONTROLS.includes(file), "STOP: separate Ops must install the literal Docs control closure");
  admission.checkout(path.resolve(__dirname, ".."), C);
  const reads = admission.createPublicReads(), { get, paged } = source.sourceReads(reads, {});
  const repository = await get(""); repo(repository); assert.equal(repository.default_branch, "main");
  assert.equal(await proof.mainAuthority(get), C, "Main moved; never deploy an older site over a newer Main");
  const runId = numeric(env.GITHUB_RUN_ID), attempt = numeric(env.GITHUB_RUN_ATTEMPT), run = await get("actions/runs/" + runId);
  positive(run.workflow_id); positive(run.check_suite_id);
  repo(run.repository); repo(run.head_repository);
  for (const [key, value] of Object.entries({ id: runId, run_attempt: attempt, name: NAME, path: WORKFLOW,
    event: "push", head_sha: C, head_branch: "main", status: "in_progress", conclusion: null })) assert.equal(run[key], value);
  const suite = source.checkedJSON(await reads.checkSuite(run.check_suite_node_id));
  assert.deepEqual(suite.repository, { databaseId: 1164779771, nameWithOwner: source.REPOSITORY });
  assert.deepEqual(suite.app, { databaseId: 15368, slug: "github-actions" }); assert.equal(suite.databaseId, run.check_suite_id);
  assert.equal(suite.commit.oid, C);
  assert.deepEqual(suite.workflowRun, { databaseId: runId, event: "push", runAttempt: attempt,
    workflow: { databaseId: run.workflow_id, name: NAME }, file: { path: WORKFLOW, repositoryName: source.REPOSITORY,
      repositoryFileUrl: `https://github.com/${source.REPOSITORY}/blob/${C}/${WORKFLOW}` } });
  const workflow = await get("actions/workflows/docs.yml"); assert.equal(workflow.id, run.workflow_id);
  assert.equal(workflow.path, WORKFLOW); assert.equal(workflow.state, "active");
  const jobs = await paged(`actions/runs/${runId}/attempts/${attempt}/jobs`, "jobs");
  const selected = jobs.filter(job => job.name === COMMANDS[command][0]); assert.equal(selected.length, 1);
  const job = selected[0]; assert.equal(job.run_id, runId); assert.equal(job.run_attempt, attempt);
  const direct = await get("actions/jobs/" + positive(job.id)); assert.equal(direct.run_attempt, attempt);
  assert.equal(source.fingerprint(direct), source.fingerprint(job), "Current physical Docs consumer job changed");
  assert.equal(job.head_sha, C); assert.equal(job.head_branch, "main"); assert.equal(job.status, "in_progress"); assert.equal(job.conclusion, null);
  const steps = job.steps.filter(step => step.name === COMMANDS[command][1]); assert.equal(steps.length, 1);
  assert.equal(steps[0].status, "in_progress"); assert.equal(steps[0].conclusion, null);
  return { env, C, runId, attempt, reads };
}
async function current(ctx) {
  const { get } = source.sourceReads(ctx.reads, {});
  const repository = await get(""); repo(repository); assert.equal(repository.default_branch, "main");
  assert.equal(await proof.mainAuthority(get), ctx.C, "Main advanced before Pages deployment");
}
async function selectPromotion(ctx) {
  const { get, paged } = source.sourceReads(ctx.reads, {}), candidates = [], ids = new Set();
  for (let page = 1; page <= 20; page++) {
    const rows = await get(`commits/${ctx.C}/pulls?per_page=100&page=${page}`); assert.ok(Array.isArray(rows) && rows.length <= 100);
    for (const pr of rows) { positive(pr.number); assert.ok(!ids.has(pr.number)); ids.add(pr.number);
      if (pr.state === "closed" && pr.base.ref === "main" && pr.merge_commit_sha === ctx.C) candidates.push(pr.number); }
    if (rows.length < 100) break;
    assert.ok(page < 20, "Main association inventory is truncated");
  }
  assert.equal(candidates.length, 1, "Require one actual merged Main PR, not a branch or tree heuristic");
  const pr = await get("pulls/" + candidates[0]); repo(pr.base.repo); repo(pr.head.repo);
  assert.equal(pr.state, "closed"); assert.equal(pr.merged, true); assert.equal(pr.base.ref, "main"); assert.equal(pr.merge_commit_sha, ctx.C);
  assert.match(pr.head.sha, SHA); assert.match(pr.head.ref, /^[A-Za-z0-9_./-]{1,255}$/);
  const locator = proof.mainLocator(pr.body);
  // Only a mode recognized by the shared parser can reach this skip. Unknown
  // modes throw there; bootstrap/controller maintenance never authorize assets.
  if (locator.mode !== "promotion") return { mode: locator.mode };
  const workflow = await get("actions/workflows/main-admission.yml"); positive(workflow.id);
  const rows = await paged(`actions/workflows/${workflow.id}/runs?event=pull_request_target&head_sha=${pr.head.sha}`, "workflow_runs");
  const runs = rows.filter(run => run.head_sha === pr.head.sha && run.head_branch === pr.head.ref && run.event === "pull_request_target" &&
    Date.parse(run.created_at) <= Date.parse(pr.merged_at)).sort((a, b) => Date.parse(b.created_at) - Date.parse(a.created_at) || b.id - a.id);
  assert.ok(runs.length); assert.equal(runs[0].status, "completed"); assert.equal(runs[0].conclusion, "success", "Never fall back to an older green admission");
  return { mode: "promotion", pullRequest: pr.number, admissionRun: positive(runs[0].id) };
}
async function merged(ctx) {
  await current(ctx); const selected = await selectPromotion(ctx);
  assert.equal(selected.mode, "promotion", "Only a published normal promotion can deploy documentation");
  const facts = await proof.verifyMergedPromotion({ pullRequest: selected.pullRequest, admissionRun: selected.admissionRun }, ctx.reads);
  assert.equal(facts.publicationAuthorized, false); assert.equal(facts.promotion.mergeRevision, ctx.C);
  await current(ctx); return facts;
}
const artifactView = value => Object.fromEntries(["id", "name", "size_in_bytes", "digest", "expired", "created_at", "updated_at", "workflow_run"].map(key => [key, value[key]]));
async function origin(ctx, facts) {
  const docs = facts.featureDocs, { get, paged } = source.sourceReads(ctx.reads, {});
  assert.equal(docs.kind, "feature-docs-execution-locator/v1");
  assert.equal(docs.runId, facts.feature.runId); assert.equal(docs.snapshotRevision, facts.feature.snapshotRevision);
  assert.equal(docs.sourceTree, facts.source.tree); assert.equal(docs.producerDigest, facts.feature.producerDigest);
  assert.equal(docs.artifactName, `feature-docs-${docs.runId}-${docs.physicalAttempt}`);
  const job = await get("actions/jobs/" + positive(docs.jobId)), selected = facts.feature.physicalJobs.filter(value => value.name === "Documentation");
  assert.equal(selected.length, 1); assert.equal(selected[0].jobId, job.id); assert.equal(job.run_id, docs.runId);
  assert.equal(selected[0].physicalAttempt, docs.physicalAttempt);
  assert.equal(job.run_attempt, docs.physicalAttempt); assert.equal(job.head_sha, facts.feature.headRevision);
  assert.equal(job.name, "Documentation"); assert.equal(job.status, "completed"); assert.equal(job.conclusion, "success");
  const execution = source.executionKey(job); assert.ok(execution);
  assert.equal(hash(Buffer.from(execution)), selected[0].stepsFingerprint);
  for (const name of STEPS) { const rows = job.steps.filter(step => step.name === name); assert.equal(rows.length, 1); assert.equal(rows[0].conclusion, "success"); }
  const upload = job.steps.find(step => step.name === UPLOAD), log = await ctx.reads.jobLog(job.id);
  assert.ok(typeof log === "string" && Buffer.byteLength(log) <= 64 * 1024 ** 2);
  // REST step bounds have second precision; server log lines may have fractions.
  const second = value => Math.floor(Date.parse(value) / 1000), start = second(upload.started_at), end = second(upload.completed_at);
  assert.ok(Number.isSafeInteger(start) && Number.isSafeInteger(end) && end >= start);
  const inWindow = value => Number.isSafeInteger(second(value)) && second(value) >= start && second(value) <= end;
  const lines = log.split(/\r?\n/).flatMap(line => {
    const match = line.match(/^(\d{4}-\d\d-\d\dT\S+Z) (.*)$/);
    return match && inWindow(match[1]) ? [match[2]] : [];
  });
  const escaped = docs.artifactName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const records = lines.map(line => line.match(new RegExp(`^Artifact ${escaped} has been successfully uploaded! Final size is ([1-9][0-9]*) bytes\\. Artifact ID is ([1-9][0-9]*)$`))).filter(Boolean);
  assert.equal(records.length, 1, "Missing/ambiguous successful physical upload record");
  const size = numeric(records[0][1]), artifactId = numeric(records[0][2]); assert.ok(size <= LIMIT);
  assert.equal(lines.filter(line => line === `Artifact download URL: https://github.com/${source.REPOSITORY}/actions/runs/${docs.runId}/artifacts/${artifactId}`).length, 1);
  const artifacts = await paged(`actions/runs/${docs.runId}/artifacts`, "artifacts"), matches = artifacts.filter(value => value.name === docs.artifactName);
  assert.equal(matches.length, 1, "Original Docs artifact is missing or ambiguous"); const artifact = matches[0];
  assert.equal(artifact.id, artifactId); assert.equal(artifact.size_in_bytes, size); assert.equal(artifact.expired, false, "Expired original Docs artifact; never rebuild");
  assert.match(artifact.digest, /^sha256:[0-9a-f]{64}$/); assert.equal(artifact.workflow_run.id, docs.runId);
  assert.equal(artifact.workflow_run.repository_id, 1164779771); assert.equal(artifact.workflow_run.head_repository_id, 1164779771);
  assert.equal(artifact.workflow_run.head_sha, facts.feature.headRevision);
  for (const key of ["created_at", "updated_at"]) assert.ok(inWindow(artifact[key]));
  assert.deepEqual(artifactView(await get("actions/artifacts/" + artifact.id)), artifactView(artifact));
  return artifact;
}
function verifyFiles(root, command) {
  const result = spawnSync("python3", ["-I", path.join(__dirname, "docs-deploy-verify.py"), command, root], {
    env: admission.childEnv(process.env), encoding: "utf8", timeout: 120000, maxBuffer: 1024 * 1024 });
  assert.ok(!result.error && result.status === 0, "Retained documentation data verification failed");
  return command === "verify" ? JSON.parse(result.stdout) : undefined;
}
function expectedHeader(facts) {
  return { snapshot: facts.feature.snapshotRevision, tree: facts.source.tree, workflowDefinition: facts.feature.workflowDefinitionRevision,
    pr: facts.feature.pr, run: facts.featureDocs.runId, attempt: facts.featureDocs.physicalAttempt, producerDigest: facts.feature.producerDigest };
}
async function expectedFiles(ctx, facts, root) {
  const receipt = jsonFile(path.join(root, "receipt.json"), 32 * 1024 ** 2), members = receipt.configuration?.defaultMembers;
  assert.ok(Array.isArray(members) && members.length === 30);
  const paths = new Set(["Cargo.toml", "Cargo.lock", ".cargo/config.toml", source.WORKFLOW]);
  for (const member of members) {
    assert.ok(typeof member.manifest === "string" && /^(?:[A-Za-z0-9_.-]+\/)*Cargo\.toml$/.test(member.manifest) && !member.manifest.split("/").includes(".."));
    paths.add(member.manifest);
  }
  const { blob } = source.sourceReads(ctx.reads, {}), sourceFiles = {};
  for (const name of paths) { const value = await blob(name, facts.source.revision); sourceFiles[name] = { sha256: value.sha256, text: value.bytes.toString("utf8") }; }
  const value = { ...expectedHeader(facts), sourceFiles };
  const bytes = Buffer.from(core.canonicalJson(value)); assert.ok(bytes.length <= 24 * 1024 ** 2);
  const file = path.join(root, "expected.json");
  if (fs.existsSync(file)) { const stat = fs.lstatSync(file); assert.ok(stat.isFile() && !stat.isSymbolicLink() && stat.nlink === 1); }
  fs.writeFileSync(file, bytes, { mode: 0o600, flag: fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_TRUNC | fs.constants.O_NOFOLLOW });
}
async function inspect(ctx) {
  const selected = await selectPromotion(ctx); await current(ctx);
  output(ctx.env, { mode: selected.mode });
  if (selected.mode !== "promotion") console.log("Documentation deployment skipped: recognized non-publication Main event; no artifact authorization.");
}
async function prepare(ctx) {
  const facts = await merged(ctx), artifact = await origin(ctx, facts), root = directory(ctx, true);
  const bytes = await ctx.reads.artifactBytes(artifact.id, artifact.size_in_bytes); assert.ok(Buffer.isBuffer(bytes));
  assert.equal(bytes.length, artifact.size_in_bytes); assert.equal(hash(bytes), artifact.digest.slice(7));
  fs.writeFileSync(path.join(root, "origin.zip"), bytes, { flag: "wx", mode: 0o600 });
  verifyFiles(root, "unpack"); await expectedFiles(ctx, facts, root); const verified = verifyFiles(root, "verify");
  assert.deepEqual(artifactView(await origin(ctx, facts)), artifactView(artifact)); await current(ctx);
  output(ctx.env, { site_tar: path.join(root, "artifact.tar"), transfer_name: `github-pages-${ctx.runId}-${ctx.attempt}` });
  console.log(`Retained documentation verified: source=${facts.source.revision} original_job=${facts.featureDocs.jobId} artifact=${artifact.id} tar_sha256=${verified.tarSha256}`);
}
async function guard(ctx) {
  const root = directory(ctx), zipSha256 = hash(fileBytes(path.join(root, "origin.zip"), LIMIT));
  const verified = verifyFiles(root, "verify"), expected = jsonFile(path.join(root, "expected.json"), 24 * 1024 ** 2);
  // Preliminary local integrity grants nothing. Authenticate Main after the
  // large local reads, then bind the held source data to the immutable S tree.
  const facts = await merged(ctx), artifact = await origin(ctx, facts), { sourceFiles, ...header } = expected;
  assert.equal(zipSha256, artifact.digest.slice(7)); assert.deepEqual(header, expectedHeader(facts));
  const { get } = source.sourceReads(ctx.reads, {}), leaves = await proof.treeLeaves(get, facts.source.tree);
  for (const [name, value] of Object.entries(sourceFiles)) {
    const bytes = Buffer.from(value.text), oid = require("node:crypto").createHash("sha1").update(`blob ${bytes.length}\0`).update(bytes).digest("hex");
    const entry = leaves.get(name); assert.ok(entry && ["100644", "100755"].includes(entry.mode)); assert.equal(entry.sha, oid);
  }
  const ownId = numeric(ctx.env.PAGES_ARTIFACT_ID), own = await ctx.reads.json("actions/artifacts/" + ownId);
  assert.equal(own.id, ownId);
  assert.equal(own.name, `github-pages-${ctx.runId}-${ctx.attempt}`); assert.equal(own.expired, false);
  assert.equal(own.workflow_run.id, ctx.runId); assert.equal(own.workflow_run.head_sha, ctx.C);
  assert.equal(own.workflow_run.repository_id, 1164779771); assert.equal(own.workflow_run.head_repository_id, 1164779771);
  assert.match(ctx.env.PAGES_ARTIFACT_SHA256, DIGEST); assert.equal(own.digest, "sha256:" + ctx.env.PAGES_ARTIFACT_SHA256);
  assert.ok(own.size_in_bytes > 0 && own.size_in_bytes <= LIMIT);
  await current(ctx);
  console.log(`Pages transfer ready: artifact=${own.id} source=${facts.source.revision} original_tar_sha256=${verified.tarSha256}`);
}
async function main() {
  assert.equal(process.argv.length, 3); const ctx = await createContext();
  await ({ inspect, prepare, guard })[process.argv[2]](ctx);
}
module.exports = { WORKFLOW, NAME, CONTROLS, LIMIT, UPLOAD, STEPS, COMMANDS, fileBytes, directory, createContext,
  selectPromotion, merged, origin, artifactView, verifyFiles, expectedFiles, inspect, prepare, guard };
if (require.main === module) main().catch(error => { console.error("Retained documentation stopped: " + error.message); process.exitCode = 1; });
