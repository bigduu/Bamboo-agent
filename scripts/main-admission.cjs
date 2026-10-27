#!/usr/bin/env node
"use strict";
// Trusted B code only. Git objects, locators and original outputs are read as data.
const assert = require("node:assert/strict");
const fs = require("node:fs"), path = require("node:path");
const { spawnSync } = require("node:child_process");
const core = require("./crate-release.cjs"), source = require("./frozen-source-admission.cjs");
const proof = require("./published-source-admission.cjs");
const LEGACY_BASE = "d6325a732c7669f94962f604e4ce98966e374d73";
const LEGACY_TREE = "9557a389d155c6be1bfb7072d3eae82e12468ff2";
const REPOSITORY_ID = 1164779771, SHA = /^[0-9a-f]{40}$/;
const hash = value => core.sha256(Buffer.isBuffer(value) ? value : Buffer.from(core.canonicalJson(value)));
const id = value => { assert.match(String(value), /^[1-9][0-9]*$/); assert.ok(Number.isSafeInteger(Number(value))); return Number(value); };
const sha = value => { assert.match(value || "", SHA); return value; };
function childEnv(env, github = false) {
  const result = Object.fromEntries(["PATH", "HOME", "TMPDIR"].filter(key => env[key] !== undefined).map(key => [key, env[key]]));
  if (github && env.GH_TOKEN) result.GH_TOKEN = env.GH_TOKEN;
  result.GH_PROMPT_DISABLED = "1"; return result;
}
function createReads(env = process.env, spawn = spawnSync, fetcher = fetch) {
  const gh = (args, maximum = 24 * 1024 * 1024, binary = false) => {
    const result = spawn("gh", ["api", "--hostname", "github.com", ...args], {
      env: childEnv(env, true), encoding: binary ? null : "utf8", timeout: 30000, maxBuffer: maximum });
    assert.ok(!result.error && result.status === 0, "Read-only GitHub request failed");
    assert.ok(Buffer.byteLength(result.stdout) <= maximum, "Oversized GitHub response"); return result.stdout;
  };
  return {
    json: async route => {
      assert.ok(typeof route === "string" && /^(?:[A-Za-z0-9_.?=&%+/-]*)$/.test(route) && !route.startsWith("/") &&
        !route.split("/").includes("..") && !/%2e/i.test(route), "Invalid fixed-repository read route");
      return JSON.parse(gh(["--method", "GET", "repos/" + source.REPOSITORY + (route ? "/" + route : "")]));
    },
    checkSuite: async nodeId => {
      assert.match(nodeId, /^[A-Za-z0-9_=-]{1,128}$/);
      const query = "query($suite:ID!){node(id:$suite){... on CheckSuite{databaseId repository{databaseId nameWithOwner} " +
        "app{databaseId slug} commit{oid} workflowRun{databaseId event runAttempt workflow{databaseId name} file{path repositoryName repositoryFileUrl}}}}}";
      const result = JSON.parse(gh(["graphql", "-f", "query=" + query, "-f", "suite=" + nodeId]));
      assert.ok(!result.errors, "Official read-only check-suite query failed"); return result.data?.node;
    },
    jobLog: async jobId => gh(["--method", "GET", `repos/${source.REPOSITORY}/actions/jobs/${id(jobId)}/logs`], 64 * 1024 * 1024),
    assetBytes: async (assetId, maximum) => {
      assert.ok(Number.isSafeInteger(maximum) && maximum > 0 && maximum <= 1024 * 1024 ** 2);
      return gh(["--method", "GET", `repos/${source.REPOSITORY}/releases/assets/${id(assetId)}`, "-H", "Accept: application/octet-stream"], maximum, true);
    },
    artifactBytes: async (artifactId, maximum) => {
      assert.ok(Number.isSafeInteger(maximum) && maximum > 0 && maximum <= 1024 * 1024 ** 2);
      return gh(["--method", "GET", `repos/${source.REPOSITORY}/actions/artifacts/${id(artifactId)}/zip`], maximum, true);
    },
    registryChecksum: async (name, version) => {
      assert.match(name, /^[A-Za-z0-9_-]{1,64}$/); core.assertVersion(version);
      const response = await fetcher(`https://crates.io/api/v1/crates/${name}/${encodeURIComponent(version)}`, {
        redirect: "error", signal: AbortSignal.timeout(30000), headers: { Accept: "application/json" } });
      assert.equal(response.status, 200); const chunks = []; let size = 0;
      for await (const chunk of response.body) { size += chunk.length; assert.ok(size <= 1024 * 1024); chunks.push(Buffer.from(chunk)); }
      const value = JSON.parse(Buffer.concat(chunks).toString("utf8")).version;
      assert.equal(value.crate, name); assert.equal(value.num, version); assert.match(value.checksum, /^[0-9a-f]{64}$/); return value.checksum;
    },
  };
}
function createPublicReads() {
  assert.equal(arguments.length, 0, "Public reads accept no policy, authority or credential override");
  return createReads();
}
function checkout(root, B, env = process.env, spawn = spawnSync) {
  root = fs.realpathSync(root);
  const git = args => {
    const result = spawn("git", args, { cwd: root, env: childEnv(env), encoding: null, timeout: 30000, maxBuffer: 32 * 1024 * 1024 });
    assert.ok(!result.error && result.status === 0, "Trusted Git read failed"); return result.stdout;
  };
  assert.equal(git(["rev-parse", "HEAD"]).toString().trim(), sha(B));
  assert.equal(git(["status", "--porcelain", "--untracked-files=all"]).toString().trim(), "", "Trusted B checkout must stay clean");
  for (const file of proof.MAIN_CONTROLS) {
    const target = path.join(root, file), stat = fs.lstatSync(target);
    assert.ok(stat.isFile() && !stat.isSymbolicLink() && fs.realpathSync(target).startsWith(root + path.sep), "Nonordinary local B control");
    const row = git(["ls-tree", B, "--", file]).toString().trim();
    assert.match(row, new RegExp("^100(?:644|755) blob [0-9a-f]{40}\\t" + file.replace(/[.*+?^${}()|[\]\\]/g, "\\$&") + "$"));
    assert.equal(hash(fs.readFileSync(target)), hash(git(["show", B + ":" + file])), "Local B control bytes changed");
  }
  assert.equal(hash(fs.readFileSync(path.join(root, proof.MAIN_WORKFLOW))), proof.MAIN_WORKFLOW_SHA256);
}
function eventIdentity(env, event) {
  assert.equal(event.repository?.id, REPOSITORY_ID); assert.equal(event.repository.full_name, source.REPOSITORY);
  assert.equal(event.repository.default_branch, "main", "STOP: separate Ops must select default Main");
  assert.equal(env.GITHUB_REPOSITORY, source.REPOSITORY); assert.equal(env.GITHUB_EVENT_NAME, "pull_request_target");
  assert.equal(env.GITHUB_ACTIONS, "true"); assert.equal(env.GITHUB_JOB, "main-admission");
  assert.equal(env.GITHUB_REF, "refs/heads/main"); assert.equal(env.GITHUB_REF_PROTECTED, "true");
  assert.equal(env.GITHUB_WORKFLOW_REF, `${source.REPOSITORY}/${proof.MAIN_WORKFLOW}@refs/heads/main`);
  const B = sha(env.GITHUB_WORKFLOW_SHA); assert.equal(env.GITHUB_SHA, B); assert.equal(event.pull_request?.base.sha, B);
  assert.equal(event.pull_request.base.ref, "main");
  assert.ok(["opened", "synchronize", "reopened", "edited", "ready_for_review"].includes(event.action));
  const locator = proof.mainLocator(event.pull_request.body);
  assert.equal(locator.mode, "promotion", "Bootstrap requires the separate one-time trusted-D Ops export");
  return { B, F: sha(event.pull_request.head.sha), pullRequest: id(event.number), locator,
    runId: id(env.GITHUB_RUN_ID), attempt: id(env.GITHUB_RUN_ATTEMPT) };
}
async function runtime(env, event, reads) {
  const identity = eventIdentity(env, event), { B, F, pullRequest, runId, attempt } = identity;
  const { get, paged, blob } = source.sourceReads(reads, {});
  assert.equal(await proof.mainAuthority(get), B, "Main base changed after the Target event");
  const pr = await get("pulls/" + pullRequest);
  assert.equal(pr.number, pullRequest); assert.equal(pr.state, "open"); assert.equal(pr.merged, false); assert.equal(pr.draft, false); assert.equal(pr.mergeable, true);
  for (const repo of [pr.base.repo, pr.head.repo]) { assert.equal(repo.id, REPOSITORY_ID); assert.equal(repo.full_name, source.REPOSITORY); }
  assert.equal(pr.base.ref, "main"); assert.equal(pr.base.sha, B); assert.equal(pr.head.sha, F); assert.equal(pr.head.ref, event.pull_request.head.ref);
  assert.match(pr.head.ref, /^[A-Za-z0-9_./-]{1,255}$/);
  assert.deepEqual(proof.mainLocator(pr.body), identity.locator, "Locator changed after the Target event");
  assert.equal((await blob(proof.MAIN_WORKFLOW, B)).sha256, proof.MAIN_WORKFLOW_SHA256);
  const run = await get("actions/runs/" + runId);
  assert.equal(run.id, runId); assert.equal(run.run_attempt, attempt); assert.equal(run.status, "in_progress"); assert.equal(run.conclusion, null);
  assert.equal(run.head_sha, F); assert.equal(run.head_branch, pr.head.ref);
  await proof.mainSuite(run, reads);
  const jobs = await paged(`actions/runs/${runId}/attempts/${attempt}/jobs`, "jobs");
  assert.equal(jobs.length, 1); const job = jobs[0];
  assert.equal(job.name, proof.MAIN_JOB); assert.equal(job.run_id, runId); assert.equal(job.run_attempt, attempt);
  assert.equal(job.head_sha, F); assert.equal(job.head_branch, pr.head.ref); assert.equal(job.status, "in_progress"); assert.equal(job.conclusion, null);
  const steps = job.steps.filter(step => step.name === proof.MAIN_STEP); assert.equal(steps.length, 1);
  assert.equal(steps[0].status, "in_progress"); assert.equal(steps[0].conclusion, null);
  return { ...identity, M: sha(pr.merge_commit_sha), execution: { revision: B, metadataRevision: F, workflowPath: proof.MAIN_WORKFLOW,
    workflowId: id(run.workflow_id), runId, physicalAttempt: attempt, jobId: id(job.id),
    checkSuiteId: id(run.check_suite_id), checkSuiteNodeId: run.check_suite_node_id } };
}
async function executeMain(env, event, root, reads) {
  eventIdentity(env, event); checkout(root, env.GITHUB_WORKFLOW_SHA, env);
  const before = await runtime(env, event, reads), { mode, ...selected } = before.locator;
  const facts = await proof.verifyPromotion({ ...selected, pullRequest: before.pullRequest,
    baseRevision: before.B, headRevision: before.F, snapshotRevision: before.M }, reads);
  assert.deepEqual(await runtime(env, event, reads), before, "Main execution or PR changed during admission");
  checkout(root, before.B, env);
  return proof.mainFrameLine({ schemaVersion: 1, kind: proof.MAIN_KIND, publicationAuthorized: false,
    repository: { id: REPOSITORY_ID, fullName: source.REPOSITORY }, locatorSha256: hash(before.locator),
    source: { revision: facts.source.revision, tree: facts.source.tree }, release: { runId: selected.releaseRun, completionSha256: facts.completionSha256 },
    promotion: facts.promotion, execution: before.execution });
}
// Exported component for a separate guarded Ops bootstrap, not a normal CLI
// mode, status check, consumer proof or publication permission.
async function verifyBootstrap(input, reads, policy = {}) {
  assert.deepEqual(Object.keys(input).sort(), ["sourceRevision", "featurePr", "ciRun", "pullRequest", "headRevision", "snapshotRevision"].sort());
  assert.equal(policy.controlPaths, undefined, "Caller-selected control paths are forbidden");
  const { get } = source.sourceReads(reads, policy), B = LEGACY_BASE, F = sha(input.headRevision), M = sha(input.snapshotRevision);
  const locator = { mode: "bootstrap", sourceRevision: sha(input.sourceRevision), featurePr: id(input.featurePr), ciRun: id(input.ciRun) };
  const pr = await get("pulls/" + id(input.pullRequest));
  assert.equal(pr.number, input.pullRequest); assert.equal(pr.state, "open"); assert.equal(pr.merged, false); assert.equal(pr.draft, false); assert.equal(pr.mergeable, true);
  for (const repo of [pr.base.repo, pr.head.repo]) { assert.equal(repo.id, REPOSITORY_ID); assert.equal(repo.full_name, source.REPOSITORY); }
  assert.equal(pr.base.ref, "main"); assert.equal(pr.base.sha, B); assert.equal(pr.head.sha, F); assert.equal(pr.merge_commit_sha, M);
  assert.match(pr.head.ref, /^[A-Za-z0-9_./-]{1,255}$/);
  assert.deepEqual(proof.mainLocator(pr.body), locator);
  const branch = await get("branches/main"); assert.equal(branch.name, "main"); assert.equal(branch.commit.sha, B, "Bootstrap only at fixed legacy Main");
  const facts = await source.verifyAcceptedDevFacts({ sourceRevision: locator.sourceRevision, featurePr: locator.featurePr, ciRun: locator.ciRun }, reads, policy);
  const commits = await Promise.all([B,F,M].map(revision => get("git/commits/" + revision)));
  commits.forEach((commit,i) => assert.equal(commit.sha, [B,F,M][i]));
  assert.equal(commits[0].tree.sha, LEGACY_TREE); assert.deepEqual(commits[2].parents.map(parent => parent.sha), [B,F]);
  const [base, controls, head, snapshot] = await Promise.all([LEGACY_TREE, facts.source.tree, commits[1].tree.sha, commits[2].tree.sha].map(tree => proof.treeLeaves(get, tree)));
  assert.equal(base.has(proof.MAIN_WORKFLOW), false, "Bootstrap already installed"); assert.equal(base.has("scripts/main-admission.cjs"), false);
  const overlay = new Map(base), entries = proof.closure(controls);
  for (const { path, ...entry } of entries) overlay.set(path, entry);
  const key = map => core.canonicalJson([...map].sort(([a],[b]) => a.localeCompare(b)));
  assert.equal(key(head), key(overlay), "Bootstrap imports product/locks or partial controls"); assert.equal(key(snapshot), key(head));
  const workflow = await source.sourceReads(reads, policy).blob(proof.MAIN_WORKFLOW, locator.sourceRevision);
  assert.equal(workflow.sha256, proof.MAIN_WORKFLOW_SHA256);
  const fresh = await get("pulls/" + input.pullRequest);
  for (const field of ["state", "merged", "draft", "mergeable", "merge_commit_sha"]) assert.deepEqual(fresh[field], pr[field]);
  const branchIdentity = value => ({ ref: value.ref, sha: value.sha, repository: { id: value.repo.id, full_name: value.repo.full_name } });
  assert.deepEqual(branchIdentity(fresh.head), branchIdentity(pr.head)); assert.deepEqual(branchIdentity(fresh.base), branchIdentity(pr.base));
  assert.deepEqual(proof.mainLocator(fresh.body), locator);
  assert.equal((await get("branches/main")).commit.sha, B);
  return { kind: "bamboo-main-bootstrap-ops/v1", publicationAuthorized: false, baseRevision: B, legacyProductTree: LEGACY_TREE,
    controllerSource: locator.sourceRevision, pullRequest: input.pullRequest, headRevision: F, snapshotRevision: M,
    snapshotTree: commits[2].tree.sha, controlsSha256: hash(entries), feature: facts.feature };
}
async function main() {
  assert.equal(arguments.length, 0); assert.equal(process.argv.length, 2, "No CLI mode or policy injection");
  const env = process.env, event = JSON.parse(fs.readFileSync(env.GITHUB_EVENT_PATH, "utf8"));
  eventIdentity(env, event); // Stop on default dev before loading any checkout or making a read.
  console.log(await executeMain(env, event, path.resolve(__dirname, ".."), createPublicReads()));
}
if (require.main === module) main().catch(error => { console.error("Bamboo Main admission: " + error.message); process.exitCode = 1; });
module.exports = { LEGACY_BASE, LEGACY_TREE, childEnv, createReads, createPublicReads, checkout, eventIdentity, runtime, executeMain, verifyBootstrap };
