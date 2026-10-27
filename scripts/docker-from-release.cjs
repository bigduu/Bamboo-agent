#!/usr/bin/env node
"use strict";
// Distribution consumes authenticated originals. This module has no source
// writer, allocator, compiler, frontend builder or original-output lifecycle.
const assert = require("node:assert/strict"), fs = require("node:fs"), path = require("node:path");
const { spawnSync } = require("node:child_process");
const core = require("./crate-release.cjs"), sidecar = require("./sidecar-release.cjs");
const proof = require("./published-source-admission.cjs"), native = require("./native-originals.cjs");
const source = require("./frozen-source-admission.cjs");
const ROOT = path.resolve(__dirname, ".."), WORKFLOW = ".github/workflows/docker-publish.yml";
// Official registry index/config metadata read 2026-10-27; both architectures
// resolve to Node24.22.1. No image layer was downloaded to choose this pin.
const RUNTIME = "node:24-bookworm-slim@sha256:55fb5f5a527ea2c5c4a95f963e48ef90aa38c7b346054fdc8ee9957a2b55d205";
const PLATFORMS = Object.freeze(["linux/amd64", "linux/arm64"]);
const IMAGE = "ghcr.io/" + sidecar.REPOSITORY.split("/")[0].toLowerCase() + "/bamboo";
const SHA = /^[0-9a-f]{40}$/, DIGEST = /^sha256:[0-9a-f]{64}$/;
const SELECT = "Select original image output", BUILD = "Assemble runtime image locally", SMOKE = "Verify loader, version, API, frontend and Host runtime";
const PUSH = "Recheck source and push candidate digest", UPLOAD = "Retain this architecture digest", REUSE = "Verify retained original image digest";
const SOURCE_JOB = "Authenticate completed release", SOURCE_STEP = "Read original completed publication", SOURCE_PREFIX = "Bamboo Docker source: ";
const ARM_JOB = "Produce Docker ARM executable original", ARM_UPLOAD = "Retain ARM binary and receipt";
const IMAGE_STEPS = [SELECT, "Copy authenticated binary and Host originals", BUILD, SMOKE, PUSH, UPLOAD];
const IMAGE_MATRIX_NAME = "Repackage originals (${{ matrix.platform }})";
const IMAGE_MATRIX_DEFINITION = "d73e12ecbc812790eb16e488ae56db50691cf606249a6584ae9b8e983b621218";
const SOURCE_KIND = "bamboo-docker-source-proof/v1", liveContexts = new WeakMap(), sourceFrames = new WeakMap();
const hashData = value => core.sha256(Buffer.from(core.canonicalJson(value)));
const jobDigest = job => core.sha256(Buffer.from(source.fingerprint(job)));
const exact = (value, names) => assert.deepEqual(Object.keys(value || {}).sort(), [...names].sort());
const stableRun = run => ({ ...Object.fromEntries(["id", "workflow_id", "check_suite_id", "check_suite_node_id", "event", "head_sha", "head_branch", "path", "name", "created_at"].map(key => [key, run[key]])),
  repository: { id: run.repository.id, full_name: run.repository.full_name }, head_repository: { id: run.head_repository.id, full_name: run.head_repository.full_name } });
const featurePr = pr => ({ number: pr.number, state: pr.state, draft: pr.draft, merged: pr.merged, merged_at: pr.merged_at,
  merge_commit_sha: pr.merge_commit_sha, head: { ref: pr.head.ref, sha: pr.head.sha, repo: { id: pr.head.repo.id, full_name: pr.head.repo.full_name } },
  base: { ref: pr.base.ref, repo: { id: pr.base.repo.id, full_name: pr.base.repo.full_name } } });
const positive = value => { assert.match(String(value), /^[1-9][0-9]*$/); assert.ok(Number.isSafeInteger(Number(value))); return Number(value); };
const safeEnv = () => Object.fromEntries(["PATH", "HOME", "TMPDIR", "LANG", "LC_ALL", "SystemRoot"].filter(key => process.env[key] !== undefined).map(key => [key, process.env[key]]));
function command(program, args, options = {}) {
  const result = spawnSync(program, args, { env: safeEnv(), encoding: "utf8", timeout: 60000, maxBuffer: 8 * 1024 * 1024, ...options });
  assert.ok(!result.error && result.status === 0, "Artifact command failed: " + program); return result.stdout;
}
function caller() {
  const env = process.env;
  assert.equal(env.GITHUB_REPOSITORY, sidecar.REPOSITORY);
  assert.ok(["workflow_run", "workflow_dispatch"].includes(env.GITHUB_EVENT_NAME));
  assert.equal(env.GITHUB_REF, "refs/heads/main");
  assert.equal(env.GITHUB_WORKFLOW_REF, sidecar.REPOSITORY + "/" + WORKFLOW + "@refs/heads/main");
  assert.match(env.GITHUB_WORKFLOW_SHA || "", SHA); assert.equal(env.GITHUB_SHA, env.GITHUB_WORKFLOW_SHA);
  assert.equal(command("git", ["-C", ROOT, "rev-parse", "HEAD"]).trim(), env.GITHUB_WORKFLOW_SHA);
  assert.equal(command("git", ["-C", ROOT, "status", "--porcelain", "--untracked-files=all"]).trim(), "");
  return env.GITHUB_WORKFLOW_SHA;
}
function eventRun(eventName, event, selected) {
  assert.ok(["workflow_run", "workflow_dispatch"].includes(eventName));
  const run = eventName === "workflow_run" ? positive(event.workflow_run?.id) : positive(event.inputs?.release_run);
  if (eventName === "workflow_run") assert.equal(event.workflow_run.conclusion, "success");
  if (selected !== undefined) assert.equal(positive(selected), run, "Run-bound Docker selector changed");
  return run;
}
async function locate(reads, runId) {
  const rows = []; let total;
  for (let page = 1; page <= 20; page++) {
    const value = await reads.json(`actions/runs/${runId}/jobs?filter=all&per_page=100&page=${page}`);
    assert.ok(Number.isSafeInteger(value.total_count) && Array.isArray(value.jobs) && value.jobs.length <= 100);
    total ??= value.total_count; assert.equal(value.total_count, total); rows.push(...value.jobs);
    if (rows.length === total) break;
    assert.equal(value.jobs.length, 100, "Incomplete finalizer locator"); assert.ok(page < 20);
  }
  assert.equal(rows.length, total); assert.equal(new Set(rows.map(job => positive(job.id))).size, rows.length);
  const family = rows.filter(job => job.name === "Publish and verify complete executable assets");
  const latest = [...family].sort((a,b) => b.run_attempt - a.run_attempt)[0]; assert.ok(latest);
  const key = native.executionKey(latest);
  const physical = family.filter(job => job.status === "completed" && job.conclusion === "success" && native.executionKey(job) === key)
    .sort((a,b) => a.run_attempt - b.run_attempt)[0]; assert.ok(physical);
  // This frame supplies locators only. The real public verifier below checks
  // the entire original C execution/source/assets independently before use.
  const record = proof.frame(await reads.jobLog(physical.id)); assert.equal(record.controller.runId, runId);
  return { sourceRevision: record.source.revision, featurePr: positive(record.source.featurePr), ciRun: positive(record.source.featureRunId), releaseRun: runId };
}
async function authenticated() {
  assert.equal(arguments.length, 0, "No supplied proof, policy or authority override");
  const state = await authenticatedSource(), record = sourceFrames.get(state);
  if (record.mode === "reuse") await freshSourceFacts(state, record.data);
  return state;
}
// Stage may read authenticated immutable originals into owned scratch before
// checking mutable authority at its tail. This private loader grants no output
// or write permission; the public factory always performs the full live guard.
async function authenticatedSource() {
  assert.equal(arguments.length, 0);
  const controller = caller(), env = process.env, event = JSON.parse(fs.readFileSync(env.GITHUB_EVENT_PATH, "utf8"));
  const runId = eventRun(env.GITHUB_EVENT_NAME, event, env.DOCKER_RELEASE_RUN);
  // The inherited trusted adapter has no policy/token/fixture override.
  const reads = require("./main-admission.cjs").createPublicReads();
  const state = { controller, reads }, context = await consumerExecution(state, env.GITHUB_JOB);
  const family = context.jobs.filter(job => job.name === SOURCE_JOB && job.id !== context.current.id);
  let first = env.GITHUB_JOB === "source" && context.run.run_attempt === 1 && !family.length;
  if (env.GITHUB_JOB === "source" && !first && !family.some(job => job.status === "completed" && job.conclusion === "success")) {
    await unreadSourceRetry(context); first = true;
  }
  if (first) await firstSource(state, context, runId);
  else {
    const held = await readSourceHandoff(state, context, env.GITHUB_JOB === "source");
    Object.assign(state, { input: held.data.input, facts: held.data.facts });
    assert.equal(state.input.releaseRun, runId, "Original Source selected a different release run");
    sourceFrames.set(state, { ...held, mode: "reuse", job: { id: context.current.id, attempt: context.current.run_attempt } });
  }
  assert.equal(state.facts.publicationAuthorized, false); core.assertVersion(state.facts.release.version);
  liveContexts.set(state, context); return state;
}
async function unreadSourceRetry(context) {
  // A failed read-only proof may retry only before any downstream execution.
  // Unknown matrix placeholders/missing jobs are not proof of no output.
  assert.ok(context.run.run_attempt >= 2 && context.run.run_attempt <= 20);
  const fixed = [SOURCE_JOB, ARM_JOB, "Publish complete multi-arch manifest", "Scan exact published image (Trivy)"];
  for (let attempt = 1; attempt < context.run.run_attempt; attempt++) {
    const rows = context.jobs.filter(job => job.run_attempt === attempt);
    const placeholder = rows.some(job => job.name === IMAGE_MATRIX_NAME);
    if (placeholder) {
      const file = path.join(ROOT, WORKFLOW), stat = fs.lstatSync(file); assert.ok(stat.isFile() && !stat.isSymbolicLink());
      const text = fs.readFileSync(file, "utf8"), marker = "\n  image:\n"; assert.equal(text.split(marker).length, 2);
      const start = text.indexOf(marker), end = text.indexOf("\n    env:\n", start); assert.ok(end > start);
      // Exact name/needs/permissions/two-platform matrix from clean, bound D.
      assert.equal(core.sha256(Buffer.from(text.slice(start, end))), IMAGE_MATRIX_DEFINITION, "Unproven image matrix definition");
    }
    const names = [...fixed, ...(placeholder ? [IMAGE_MATRIX_NAME] : PLATFORMS.map(platform => "Repackage originals (" + platform + ")"))].sort();
    assert.deepEqual(rows.map(job => job.name).sort(), names, "Incomplete or unknown prior Source/downstream history");
    const official = await context.paged(`actions/runs/${context.run.id}/attempts/${attempt}/jobs`, "jobs");
    assert.equal(official.length, rows.length);
    for (const job of rows) {
      const matches = official.filter(value => value.id === job.id); assert.equal(matches.length, 1);
      assert.equal(matches[0].run_attempt, attempt); assert.equal(jobDigest(matches[0]), jobDigest(job));
      assert.equal(job.status, "completed");
      if (job.name === SOURCE_JOB) {
        assert.ok(["failure", "cancelled", "timed_out", "skipped"].includes(job.conclusion));
        const direct = await context.get("actions/jobs/" + job.id);
        assert.equal(direct.run_attempt, attempt); assert.equal(jobDigest(direct), jobDigest(job));
      } else {
        assert.equal(job.conclusion, "skipped", "A started worker forbids another full Source proof");
        assert.ok(Array.isArray(job.steps) && job.steps.length === 0, "Skipped worker must have no executed steps");
      }
    }
  }
}
function parseSourceFrame(log) {
  assert.ok(typeof log === "string" && Buffer.byteLength(log) <= 64 * 1024 * 1024);
  const rows = log.split(/\r?\n/).filter(line => /^\d{4}-\d\d-\d\dT\S+Z /.test(line) && line.slice(line.indexOf(" ") + 1).startsWith(SOURCE_PREFIX));
  assert.equal(rows.length, 1, "Missing/ambiguous Source handoff");
  const encoded = rows[0].slice(rows[0].indexOf(SOURCE_PREFIX) + SOURCE_PREFIX.length);
  assert.ok(encoded.length <= 87384 && /^[A-Za-z0-9+/]+={0,2}$/.test(encoded));
  const bytes = Buffer.from(encoded, "base64"); assert.ok(bytes.length <= 65536); assert.equal(bytes.toString("base64"), encoded);
  const value = JSON.parse(bytes.toString("utf8")); assert.equal(core.canonicalJson(value), bytes.toString("utf8"));
  exact(value, ["schemaVersion", "kind", "publicationAuthorized", "mode", "selection", "job", "origin", "data"]);
  assert.equal(value.schemaVersion, 1); assert.equal(value.kind, SOURCE_KIND); assert.equal(value.publicationAuthorized, false);
  assert.ok(["full", "reuse"].includes(value.mode));
  for (const item of [value.job, value.origin]) { exact(item, ["id", "attempt"]); positive(item.id); positive(item.attempt); }
  assert.equal(value.mode === "full", value.data !== null); return value;
}
function sourcePhysical(context, allowRecovery) {
  const rows = context.jobs.filter(job => job.name === SOURCE_JOB && (!allowRecovery || job.id !== context.current.id));
  assert.ok(rows.length, "No original successful Source; workers never run a full-proof fallback");
  assert.equal(new Set(rows.map(job => job.run_attempt)).size, rows.length);
  const latest = [...rows].sort((a,b) => b.run_attempt - a.run_attempt)[0];
  if (!allowRecovery) passed(latest);
  const candidates = allowRecovery ? rows.filter(job => job.status === "completed" && job.conclusion === "success") : [latest];
  const selected = [...candidates].sort((a,b) => b.run_attempt - a.run_attempt)[0]; assert.ok(selected);
  const key = source.executionKey(selected); assert.ok(key);
  const physical = rows.filter(job => source.executionKey(job) === key).sort((a,b) => a.run_attempt - b.run_attempt)[0];
  passed(physical); passed(step(physical, SOURCE_STEP)); return physical;
}
async function readSourceHandoff(state, context, allowRecovery = false) {
  const { get, paged } = context, attempts = new Map();
  const read = async physical => {
    jobHeader(physical, context.run); passed(physical); passed(step(physical, SOURCE_STEP));
    assert.equal(physical.name, SOURCE_JOB); assert.ok(Date.parse(physical.started_at) >= Date.parse(context.run.created_at));
    const direct = await get("actions/jobs/" + physical.id); assert.equal(direct.run_attempt, physical.run_attempt); assert.equal(jobDigest(direct), jobDigest(physical));
    if (!attempts.has(physical.run_attempt)) attempts.set(physical.run_attempt, await paged(`actions/runs/${context.run.id}/attempts/${physical.run_attempt}/jobs`, "jobs"));
    const members = attempts.get(physical.run_attempt).filter(job => job.id === physical.id); assert.equal(members.length, 1);
    assert.equal(members[0].run_attempt, physical.run_attempt); assert.equal(jobDigest(members[0]), jobDigest(physical));
    const log = await state.reads.jobLog(physical.id), value = parseSourceFrame(log);
    sourceLocator(log, context.run, physical);
    assert.deepEqual(value.job, { id: physical.id, attempt: physical.run_attempt });
    assert.equal(value.selection.controller, state.controller); assert.equal(value.selection.consumerRun, context.run.id);
    return value;
  };
  const latest = await read(sourcePhysical(context, allowRecovery)); let full = latest;
  if (latest.mode === "reuse") {
    const originals = context.jobs.filter(job => job.id === latest.origin.id && job.run_attempt === latest.origin.attempt);
    assert.equal(originals.length, 1, "Original full Source disappeared"); full = await read(originals[0]);
    assert.equal(full.mode, "full", "Reuse cannot invent another origin chain"); assert.deepEqual(full.selection, latest.selection);
  }
  assert.deepEqual(full.origin, full.job); assert.ok(full.data && typeof full.data === "object");
  assert.deepEqual(full.selection, { ...selected(full.data.facts), consumerRun: context.run.id, controller: state.controller });
  assert.equal(full.data.facts.publicationAuthorized, false); return full;
}
async function firstSource(state, context, runId) {
  assert.equal(process.env.GITHUB_JOB, "source"); assert.equal(step(context.current, SOURCE_STEP).status, "in_progress");
  // Observe actual reads; never return a memoized response to either verifier.
  const observed = new Map(), reads = { ...state.reads, json: async route => {
    const value = await state.reads.json(route); observed.set(route, structuredClone(value)); return value;
  } };
  const input = await locate(reads, runId), facts = await proof.verifyPublishedSource(input, reads);
  const { get, ancestor } = source.sourceReads(state.reads, {}), H = await proof.mainAuthority(get);
  if (H !== facts.mainObservedAfter) await ancestor(facts.mainObservedAfter, H);
  await ancestor(state.controller, H);
  const A = observed.get("branches/dev").commit.sha;
  const physical = ids => [...new Set(ids)].map(id => {
    const job = observed.get("actions/jobs/" + id); assert.ok(job, "Full verifier did not independently read original job");
    return { id, attempt: job.run_attempt, digest: jobDigest(job) };
  });
  const list = prefix => [...observed].filter(([route]) => route.startsWith(prefix)).sort(([a],[b]) => Number(a.split("page=").at(-1)) - Number(b.split("page=").at(-1))).flatMap(([,rows]) => rows.jobs ?? rows);
  const featureLatest = list(`actions/runs/${input.ciRun}/jobs?filter=latest&`);
  const definition = observed.get(`contents/${source.CONTROLLER}?ref=${facts.controller.revision}`);
  const body = definition.encoding === "base64" ? definition : observed.get("git/blobs/" + definition.sha);
  const data = { input, facts, mainAnchor: H, devAnchor: A,
    featureRun: source.runIdentity(observed.get("actions/runs/" + input.ciRun)), publisherRun: stableRun(observed.get("actions/runs/" + runId)),
    producer: core.sha256(Buffer.from(body.content.replace(/[\r\n]/g, ""), "base64")), pr: featurePr(observed.get("pulls/" + input.featurePr)),
    release: hashData(proof.releaseIdentity(observed.get("releases/" + facts.release.id))), assets: hashData(proof.assetIdentities(list(`releases/${facts.release.id}/assets?`))),
    featurePhysical: physical(facts.feature.physicalJobs.map(job => job.jobId)),
    featureLogical: featureLatest.map(job => ({ id: job.id, attempt: job.run_attempt, digest: jobDigest(job) })),
    releasePhysical: physical([...facts.jobs.map(job => job.physical.id), facts.controller.finalizerJobId, ...facts.crates.map(crate => crate.origin.jobId)]) };
  const direct = await get("actions/jobs/" + context.current.id); assert.equal(direct.run_attempt, context.current.run_attempt); assert.equal(jobDigest(direct), jobDigest(context.current));
  Object.assign(state, { input, facts }); const origin = { id: context.current.id, attempt: context.current.run_attempt };
  const value = { schemaVersion: 1, kind: SOURCE_KIND, publicationAuthorized: false, mode: "full",
    selection: { ...selected(facts), consumerRun: context.run.id, controller: state.controller }, job: origin, origin, data };
  parseSourceFrame("2000-01-01T00:00:00Z " + SOURCE_PREFIX + Buffer.from(core.canonicalJson(value)).toString("base64")); sourceFrames.set(state, value);
}
function officialRun(run, suite, path, definition) {
  source.checkedJSON(suite); assert.equal(run.repository?.id, 1164779771); assert.equal(run.repository.full_name, sidecar.REPOSITORY);
  assert.equal(run.head_repository?.id, 1164779771); assert.equal(run.head_repository.full_name, sidecar.REPOSITORY); assert.equal(run.path, path); passed(run); positive(run.run_attempt); assert.ok(run.run_attempt <= 20);
  assert.equal(suite.databaseId, run.check_suite_id); assert.deepEqual(suite.repository, { databaseId: 1164779771, nameWithOwner: sidecar.REPOSITORY });
  assert.deepEqual(suite.app, { databaseId: 15368, slug: "github-actions" }); assert.equal(suite.commit.oid, run.head_sha);
  const value = suite.workflowRun; assert.equal(value.databaseId, run.id); assert.equal(value.event, run.event); assert.equal(value.runAttempt, run.run_attempt);
  assert.deepEqual(value.workflow, { databaseId: run.workflow_id, name: run.name });
  assert.deepEqual(value.file, { path, repositoryName: sidecar.REPOSITORY, repositoryFileUrl: `https://github.com/${sidecar.REPOSITORY}/blob/${definition}/${path}` });
}
async function freshSourceFacts(state, data) {
  const { get, paged, ancestor, branch } = source.sourceReads(state.reads, {}), { facts, input } = state;
  const featureContract = source.ACCEPTED_PRODUCERS[facts.feature.producerDigest], releaseContract = proof.ACCEPTED_RELEASE_PRODUCERS[data.producer];
  assert.equal(featureContract?.complete, true); assert.equal(releaseContract?.complete, true);
  // Long registry reads precede live authority metadata, never follow it.
  for (const crate of facts.crates) assert.equal(await state.reads.registryChecksum(crate.name, facts.release.version), crate.registryChecksum);
  const H = await proof.mainAuthority(get); if (H !== data.mainAnchor) await ancestor(data.mainAnchor, H);
  const A = await branch("dev", true); if (A !== data.devAnchor) await ancestor(data.devAnchor, A);
  assert.equal(await branch(facts.source.releaseRef), facts.source.revision);
  assert.deepEqual(featurePr(await get("pulls/" + input.featurePr)), data.pr);
  const feature = await get("actions/runs/" + input.ciRun); assert.equal(source.runIdentity(feature), data.featureRun);
  officialRun(feature, await state.reads.checkSuite(feature.check_suite_node_id), source.WORKFLOW, facts.feature.workflowDefinitionRevision);
  const fw = await get("actions/workflows/" + feature.workflow_id); assert.equal(fw.id, feature.workflow_id); assert.equal(fw.path, source.WORKFLOW); assert.equal(fw.state, "active");
  const candidates = await paged(`actions/workflows/${feature.workflow_id}/runs?event=pull_request&head_sha=${feature.head_sha}`, "workflow_runs");
  const sameHead = candidates.filter(run => run.event === "pull_request" && run.head_sha === feature.head_sha && run.head_branch === feature.head_branch);
  assert.ok(sameHead.some(run => run.id === feature.id));
  for (const run of sameHead) assert.ok(run.id <= feature.id && Number.isFinite(Date.parse(run.created_at)) && Date.parse(run.created_at) <= Date.parse(feature.created_at), "Newer feature evidence supersedes Source");
  const checkPhysical = async (run, all, expected) => {
    const attempts = new Map();
    for (const row of expected) {
      const found = all.filter(job => job.id === row.id); assert.equal(found.length, 1, "Original physical execution disappeared");
      const job = found[0]; assert.equal(job.run_id, run.id); assert.equal(job.head_sha, run.head_sha); assert.equal(job.head_branch, run.head_branch);
      assert.equal(job.run_attempt, row.attempt); assert.equal(jobDigest(job), row.digest, "Original physical fields changed");
      if (!attempts.has(row.attempt)) attempts.set(row.attempt, await paged(`actions/runs/${run.id}/attempts/${row.attempt}/jobs`, "jobs"));
      const members = attempts.get(row.attempt).filter(item => item.id === row.id); assert.equal(members.length, 1);
      assert.equal(members[0].run_attempt, row.attempt); assert.equal(jobDigest(members[0]), row.digest);
    }
  };
  const latest = await paged(`actions/runs/${feature.id}/jobs?filter=latest`, "jobs"), all = await paged(`actions/runs/${feature.id}/jobs?filter=all`, "jobs");
  assert.equal(featureContract.requiredJobs.length, facts.feature.physicalJobs.length);
  for (const held of facts.feature.physicalJobs) {
    assert.ok(featureContract.requiredJobs.includes(held.name));
    const matches = latest.filter(job => job.name === held.name); assert.equal(matches.length, 1);
    const logical = matches[0], family = all.filter(job => job.name === held.name); assert.ok(family.length);
    assert.equal(new Set(family.map(job => job.run_attempt)).size, family.length);
    for (const job of family) { assert.equal(job.run_id, feature.id); assert.equal(job.head_sha, feature.head_sha); assert.equal(job.head_branch, feature.head_branch); positive(job.run_attempt); assert.ok(job.run_attempt <= feature.run_attempt); }
    const newest = [...family].sort((a,b) => b.run_attempt - a.run_attempt)[0]; assert.equal(jobDigest(newest), jobDigest(logical)); assert.equal(newest.run_attempt, logical.run_attempt); passed(logical);
    for (const name of featureContract.requiredSteps[held.name]) passed(step(logical, name));
    const key = source.executionKey(logical); assert.ok(key);
    const physical = family.filter(job => source.executionKey(job) === key).sort((a,b) => a.run_attempt - b.run_attempt)[0];
    assert.equal(physical.id, held.jobId); assert.equal(physical.run_attempt, held.physicalAttempt); assert.equal(core.sha256(Buffer.from(key)), held.stepsFingerprint);
    assert.equal(logical.id, held.logicalJobId); assert.equal(logical.run_attempt, held.logicalAttempt);
    const original = data.featureLogical.filter(row => row.id === logical.id); assert.equal(original.length, 1); assert.equal(jobDigest(logical), original[0].digest);
  }
  await checkPhysical(feature, all, data.featurePhysical);
  const publisher = await get("actions/runs/" + input.releaseRun); assert.deepEqual(stableRun(publisher), data.publisherRun);
  officialRun(publisher, await state.reads.checkSuite(publisher.check_suite_node_id), source.CONTROLLER, facts.controller.revision);
  const workflow = await get("actions/workflows/" + publisher.workflow_id); assert.equal(workflow.id, publisher.workflow_id); assert.equal(workflow.path, source.CONTROLLER); assert.equal(workflow.state, "active");
  const releaseLatest = await paged(`actions/runs/${publisher.id}/jobs?filter=latest`, "jobs"), releaseAll = await paged(`actions/runs/${publisher.id}/jobs?filter=all`, "jobs");
  for (const { physical } of proof.selectJobs(releaseLatest, releaseAll, publisher, releaseContract)) {
    const held = data.releasePhysical.filter(row => row.id === physical.id); assert.equal(held.length, 1, "Whole completion changed");
    assert.equal(held[0].attempt, physical.run_attempt); assert.equal(jobDigest(physical), held[0].digest);
  }
  await checkPhysical(publisher, releaseAll, data.releasePhysical);
  assert.equal(hashData(proof.frame(await state.reads.jobLog(facts.controller.finalizerJobId))), facts.completionSha256,
    "Original whole completion changed or disappeared");
  assert.equal(hashData(proof.releaseIdentity(await get("releases/" + facts.release.id))), data.release);
  const tag = await get("git/ref/tags/" + facts.release.tag); assert.equal(tag.object.type, "commit"); assert.equal(tag.object.sha, facts.source.revision);
  const assets = []; for (let page = 1; page <= 20; page++) {
    const rows = await get(`releases/${facts.release.id}/assets?per_page=100&page=${page}`); assert.ok(Array.isArray(rows) && rows.length <= 100); assets.push(...rows);
    if (rows.length < 100) break; assert.ok(page < 20);
  }
  assert.equal(new Set(assets.map(row => row.id)).size, assets.length); assert.equal(new Set(assets.map(row => row.name)).size, assets.length); assert.equal(hashData(proof.assetIdentities(assets)), data.assets);
  assert.equal(await branch(facts.source.releaseRef), facts.source.revision);
  assert.equal(source.runIdentity(await get("actions/runs/" + publisher.id)), source.runIdentity(publisher), "Publisher changed during guard");
}
async function refreshPublication(held) {
  // Re-authenticate the official Source and every live guard after long reads;
  // do not repeat its immutable full-proof downloads. No supplied JSON grants
  // authority, and the actual consumer step is checked separately at the tail.
  const fresh = await authenticated(), identity = value => ({ consumerController: value.controller, input: value.input,
    source: value.facts.source, release: value.facts.release, controller: value.facts.controller, completionSha256: value.facts.completionSha256 });
  assert.deepEqual(identity(fresh), identity(held), "Completed publication changed before registry write");
}
function target(platform) {
  assert.ok(PLATFORMS.includes(platform), "Unknown Docker architecture");
  return platform === "linux/amd64" ? "x86_64-unknown-linux-gnu" : "aarch64-unknown-linux-gnu";
}
function verifyBytes(bytes, reference) {
  assert.ok(Buffer.isBuffer(bytes) && bytes.length === reference.sizeBytes);
  assert.equal(core.sha256(bytes), reference.sha256, "Original bytes differ"); return bytes;
}
async function original(reads, reference) {
  const fields = value => ({ id: value.id, name: value.name, state: value.state, size: value.size, digest: value.digest });
  const expected = { id: positive(reference.assetId), name: reference.name, state: "uploaded", size: reference.sizeBytes, digest: "sha256:" + reference.sha256 };
  assert.deepEqual(fields(await reads.json("releases/assets/" + expected.id)), expected);
  const bytes = verifyBytes(Buffer.from(await reads.assetBytes(expected.id, expected.size)), reference);
  assert.deepEqual(fields(await reads.json("releases/assets/" + expected.id)), expected); return bytes;
}
const HOST_READER = String.raw`import base64,io,json,sys,tarfile
raw=sys.stdin.buffer.read(8*1024*1024+1); assert 0<len(raw)<=8*1024*1024
files={}; total=0
with tarfile.open(fileobj=io.BytesIO(raw),mode='r:gz') as archive:
 for member in archive:
  assert member.name in ('browser-runtime/host.cjs','browser-runtime/package.json','browser-runtime/package-lock.json')
  assert member.isfile() and not member.issparse() and member.name not in files and 0<member.size<=8*1024*1024
  total+=member.size; assert total<=8*1024*1024
  files[member.name]=base64.b64encode(archive.extractfile(member).read()).decode('ascii')
assert len(files)==3
print(json.dumps(files))`;
function hostFiles(bytes, expected) {
  assert.deepEqual(expected.map(value => value.path).sort(), [...sidecar.HOST_FILES].sort(), "Incomplete Host manifest");
  // Each member may need two padding bytes before base64. Add the exact empty
  // JSON/key overhead, Python's colon/comma spaces and at most a CRLF newline.
  const count = sidecar.HOST_FILES.length, base64Bound = 4 * Math.floor((sidecar.MAX_HOST + 2 * count) / 3);
  const jsonOverhead = Buffer.byteLength(JSON.stringify(Object.fromEntries(sidecar.HOST_FILES.map(name => [name, ""])))) + count + count - 1 + 2;
  const files = JSON.parse(command("python3", ["-I", "-c", HOST_READER], { input: bytes, maxBuffer: base64Bound + jsonOverhead }));
  assert.deepEqual(Object.keys(files).sort(), [...sidecar.HOST_FILES].sort());
  for (const name of Object.keys(files)) {
    files[name] = Buffer.from(files[name], "base64"); const found = expected.find(value => value.path === name); assert.ok(found);
    assert.equal(files[name].length, found.sizeBytes); assert.equal(core.sha256(files[name]), found.sha256);
  }
  const pkg = JSON.parse(files["browser-runtime/package.json"]), lock = JSON.parse(files["browser-runtime/package-lock.json"]);
  assert.equal(pkg.name, "bamboo-browser-runtime"); assert.equal(pkg.private, true);
  assert.deepEqual(pkg.dependencies, { "playwright-core": "1.62.1" });
  assert.equal(lock.packages["node_modules/playwright-core"].version, "1.62.1");
  assert.equal(lock.packages["node_modules/playwright-core"].resolved, "https://registry.npmjs.org/playwright-core/-/playwright-core-1.62.1.tgz");
  assert.match(lock.packages["node_modules/playwright-core"].integrity, /^sha512-[A-Za-z0-9+/]+={0,2}$/);
  return files;
}
function elfReport(report, platform) {
  assert.ok(PLATFORMS.includes(platform));
  assert.ok(typeof report === "string" && report.length <= 4 * 1024 * 1024);
  assert.match(report, /Class:\s+ELF64/); assert.match(report, platform === "linux/amd64" ? /Machine:\s+Advanced Micro Devices X86-64/ : /Machine:\s+AArch64/);
  const needed = [...report.matchAll(/\(NEEDED\).*\[([^\]]+)\]/g)].map(match => match[1]); assert.ok(needed.includes("libc.so.6"));
  const versions = [...new Set(report.match(/(?:GLIBC|GLIBCXX|CXXABI)_[0-9.]+/g) || [])].sort(); assert.ok(versions.some(value => value.startsWith("GLIBC_")));
  for (const version of versions.filter(value => value.startsWith("GLIBC_"))) {
    const [major, minor] = version.slice(6).split(".").map(Number); assert.ok(major < 2 || major === 2 && minor <= 36, "Original needs newer glibc than fixed Bookworm runtime");
  }
  return { needed, symbolVersions: versions }; // Actual loader/probe still owns compatibility acceptance.
}
function output(name, value) { assert.ok(!String(value).includes("\n")); fs.appendFileSync(process.env.GITHUB_OUTPUT, name + "=" + value + "\n"); }
function selected(facts) { return { source: facts.source.revision, version: facts.release.version, releaseRun: facts.controller.runId, publisher: facts.controller.revision, releaseId: facts.release.id }; }
async function stage() {
  const platform = process.env.DOCKER_PLATFORM, triple = target(platform), state = await authenticatedSource();
  const execution = await consumerExecution(state, "image"); passed(step(execution.current, SELECT));
  assert.ok(!imageHistory(execution.jobs, execution.run, platform, execution.run.run_attempt).physical, "Stage cannot replace an original image");
  await noPriorCarrier(execution, platform);
  const facts = state.facts, arm = platform === "linux/arm64" ? await require("./docker-arm-original.cjs").readOriginal(state, execution) : null;
  const binaryRef = arm ? arm.reference : facts.native.assets.find(value => value.name === sidecar.artifactName(triple)); assert.ok(binaryRef);
  const hostRef = facts.native.assets.find(value => value.name === sidecar.HOST_ASSET), manifestRef = facts.native.assets.find(value => value.name === sidecar.MANIFEST);
  assert.ok(hostRef && manifestRef); const manifest = JSON.parse(await original(state.reads, manifestRef));
  assert.equal(manifest.sourceRevision, facts.source.revision); assert.equal(manifest.version, facts.release.version);
  const directory = path.join(path.resolve(process.env.RUNNER_TEMP), "bamboo-docker-context"); fs.mkdirSync(directory);
  const file = path.join(directory, "bamboo");
  if (arm) { fs.copyFileSync(arm.binaryFile, file, fs.constants.COPYFILE_EXCL); fs.chmodSync(file, 0o755); }
  else fs.writeFileSync(file, await original(state.reads, binaryRef), { flag: "wx", mode: 0o755 });
  const abi = elfReport(command("readelf", ["--wide", "--file-header", "--dynamic", "--version-info", file]), platform);
  const host = hostFiles(await original(state.reads, hostRef), manifest.browserHost.files); fs.mkdirSync(path.join(directory, "browser-runtime"));
  for (const [name, bytes] of Object.entries(host)) fs.writeFileSync(path.join(directory, name), bytes, { flag: "wx", mode: 0o644 });
  fs.copyFileSync(path.join(ROOT, "docker/Dockerfile"), path.join(directory, "Dockerfile"), fs.constants.COPYFILE_EXCL);
  const descriptor = { ...selected(facts), platform, consumer: state.controller, binary: binaryRef, host: hostRef, abi, runtime: RUNTIME };
  fs.writeFileSync(path.join(directory, "originals.json"), JSON.stringify(descriptor, null, 2) + "\n", { flag: "wx" });
  await refreshPublication(state);
  await freshConsumer(execution, "Copy authenticated binary and Host originals");
  output("context", directory); output("version", facts.release.version); output("source", facts.source.revision);
  output("image", candidateImage(process.env.GITHUB_RUN_ID, process.env.GITHUB_RUN_ATTEMPT, platform));
}
function imageReference(value) { assert.match(value || "", /^(?:[a-z0-9][a-z0-9._/-]*)(?::[A-Za-z0-9_.-]+|@sha256:[0-9a-f]{64})$/); return value; }
function candidateImage(run, attempt, platform) {
  assert.ok(PLATFORMS.includes(platform)); return IMAGE + ":candidate-" + positive(run) + "-" + positive(attempt) + "-" + platform.slice(6);
}
function smokeCommands(image, version) {
  imageReference(image); core.assertVersion(version);
  const isolation = ["run", "--rm", "--init", "--network", "none", "--cap-drop", "ALL", "--security-opt", "no-new-privileges"];
  return [[...isolation, "--entrypoint", "bamboo", image, "--version"],
    [...isolation, "--entrypoint", "/bin/sh", image, "-ec", "ldd /usr/local/bin/bamboo > /tmp/loader.txt; cat /tmp/loader.txt; ! grep -q 'not found' /tmp/loader.txt; test -s \"$BAMBOO_BROWSER_HOST_SCRIPT\"; node --version | grep -Eq '^v24\\.[0-9]+\\.[0-9]+$'"],
    [...isolation, "--entrypoint", "node", image, "-e", "require(process.env.BAMBOO_BROWSER_HOST_SCRIPT);const {chromium}=require('/opt/bamboo/browser-runtime/node_modules/playwright-core');(async()=>{const b=await chromium.launch({headless:true});console.log(await b.version());await b.close()})().catch(e=>{console.error(e.message);process.exitCode=1})"],
    [...isolation, "--entrypoint", "/bin/sh", image, "-ec", String.raw`bamboo serve --port 9562 --bind 127.0.0.1 --data-dir /data >/tmp/bamboo.log 2>&1 &
p=$!
trap 'kill "$p" 2>/dev/null || :; wait "$p" 2>/dev/null || :' EXIT
i=0
until curl -fsS http://127.0.0.1:9562/api/v1/health >/dev/null && curl -fsS -D /tmp/headers -o /tmp/index http://127.0.0.1:9562/ && test -s /tmp/index && grep -Eiq '^content-type: *text/html' /tmp/headers; do
  i=$((i+1)); if test "$i" -ge 30; then cat /tmp/bamboo.log; exit 1; fi
  sleep 1
done
test -s /data/frontend/.frontend-manifest.json`]];
}
async function smoke() {
  const state = await authenticated(), image = imageReference(process.env.DOCKER_IMAGE);
  const execution = await consumerExecution(state, "image"); passed(step(execution.current, BUILD));
  assert.equal(image, candidateImage(process.env.GITHUB_RUN_ID, process.env.GITHUB_RUN_ATTEMPT, process.env.DOCKER_PLATFORM));
  const commands = smokeCommands(image, state.facts.release.version);
  const version = command("docker", commands[0]).trim(); assert.equal(version, "bamboo " + state.facts.release.version);
  for (const args of commands.slice(1)) command("docker", args);
}
function digestRecord(value, facts, platform) {
  assert.deepEqual(Object.keys(value).sort(), ["digest", "platform", "publisher", "releaseId", "releaseRun", "source", "version"].sort());
  assert.deepEqual({ ...value, digest: undefined, platform: undefined }, { ...selected(facts), digest: undefined, platform: undefined });
  assert.ok(PLATFORMS.includes(value.platform)); if (platform) assert.equal(value.platform, platform); assert.match(value.digest, DIGEST); return value;
}
function digestFile(run, attempt, platform) {
  assert.ok(PLATFORMS.includes(platform)); return `docker-digest-${positive(run)}-${positive(attempt)}-${platform.slice(6)}.json`;
}
function manifestReport(value, records) {
  assert.match(value.digest, DIGEST); assert.equal(value.manifests.length, 2);
  const actual = value.manifests.map(item => ({ platform: item.platform.os + "/" + item.platform.architecture, digest: item.digest }));
  assert.deepEqual(actual.sort((a,b) => a.platform.localeCompare(b.platform)), records.map(({ platform, digest }) => ({ platform, digest })).sort((a,b) => a.platform.localeCompare(b.platform)));
  return value.digest;
}
function latestIdentity(value) {
  assert.equal(typeof value.id, "number"); positive(value.id);
  for (const key of ["tag_name", "target_commitish"]) assert.ok(typeof value[key] === "string" && value[key].length > 0 && value[key].length <= 255);
  assert.equal(value.draft, false); assert.equal(value.prerelease, false);
  return { id: value.id, tag: value.tag_name, source: value.target_commitish, draft: value.draft, prerelease: value.prerelease };
}
function publicationTags(facts, value) {
  const latest = latestIdentity(value), tag = "v" + core.assertVersion(facts.release.version), tags = [IMAGE + ":" + tag];
  if (latest.id === facts.release.id) {
    assert.equal(latest.tag, tag); assert.equal(latest.source, facts.source.revision); tags.push(IMAGE + ":latest");
  } else assert.notEqual(latest.tag, tag, "Latest tag has a foreign Release identity");
  return tags;
}
function verifyLatest(before, after) { assert.deepEqual(latestIdentity(after), latestIdentity(before), "Official latest Release changed during Docker publication"); }
function step(job, name) { const rows = (job.steps || []).filter(value => value.name === name); assert.equal(rows.length, 1, "Missing/ambiguous Docker step: " + name); return rows[0]; }
function passed(value) { assert.equal(value.status, "completed"); assert.equal(value.conclusion, "success"); }
function skipped(value) { assert.equal(value.status, "completed"); assert.equal(value.conclusion, "skipped"); }
function jobHeader(job, run) {
  positive(job.id); positive(job.run_attempt); assert.ok(job.run_attempt <= run.run_attempt);
  assert.equal(job.run_id, run.id); assert.equal(job.head_sha, run.head_sha); assert.equal(job.head_branch, run.head_branch);
}
function consumerSuite(run, suite, exactController) {
  source.checkedJSON(suite); assert.equal(suite.databaseId, run.check_suite_id);
  assert.equal(suite.repository.databaseId, 1164779771); assert.equal(suite.repository.nameWithOwner, sidecar.REPOSITORY);
  assert.equal(suite.app.databaseId, 15368); assert.equal(suite.app.slug, "github-actions"); assert.equal(suite.commit.oid, run.head_sha);
  assert.equal(suite.workflowRun.databaseId, run.id); assert.equal(suite.workflowRun.workflow.databaseId, run.workflow_id);
  assert.equal(suite.workflowRun.event, run.event); assert.equal(suite.workflowRun.runAttempt, run.run_attempt);
  assert.equal(suite.workflowRun.file.path, WORKFLOW); assert.equal(suite.workflowRun.file.repositoryName, sidecar.REPOSITORY);
  assert.equal(suite.workflowRun.file.repositoryFileUrl, `https://github.com/${sidecar.REPOSITORY}/blob/${run.head_sha}/${WORKFLOW}`);
  if (exactController) {
    assert.equal(run.head_sha, exactController, "Unproven prior controller metadata role; stop for Ops");
    assert.equal(suite.workflowRun.file.repositoryFileUrl, `https://github.com/${sidecar.REPOSITORY}/blob/${exactController}/${WORKFLOW}`);
  }
}
async function consumerExecution(state, role) {
  assert.ok(["source", "image", "merge", "arm-originals"].includes(role), "Unknown Docker job role");
  const env = process.env; assert.equal(env.GITHUB_JOB, role, "Actual job key must match CLI phase");
  if (liveContexts.has(state)) {
    const context = liveContexts.get(state); liveContexts.delete(state);
    // Consume this authentication's local projection once. Every refresh reads
    // a new projection; actual compiler/write steps still use freshConsumer.
    if (role !== "source") {
      const fresh = await context.get("actions/runs/" + context.run.id);
      assert.deepEqual(stableRun(fresh), stableRun(context.run)); assert.equal(fresh.run_attempt, context.run.run_attempt); assert.equal(fresh.status, "in_progress");
    }
    return context;
  }
  const { get, paged } = source.sourceReads(state.reads, {}), run = await get("actions/runs/" + positive(env.GITHUB_RUN_ID));
  assert.equal(run.id, positive(env.GITHUB_RUN_ID)); assert.equal(run.run_attempt, positive(env.GITHUB_RUN_ATTEMPT)); assert.equal(run.status, "in_progress");
  assert.equal(run.repository?.id, 1164779771); assert.equal(run.repository.full_name, sidecar.REPOSITORY);
  assert.equal(run.head_repository?.id, 1164779771); assert.equal(run.head_repository.full_name, sidecar.REPOSITORY);
  assert.equal(run.event, env.GITHUB_EVENT_NAME); assert.equal(run.path, WORKFLOW); assert.match(run.head_sha, SHA);
  const workflow = await get("actions/workflows/" + positive(run.workflow_id)); assert.equal(workflow.id, run.workflow_id); assert.equal(workflow.path, WORKFLOW);
  consumerSuite(run, await state.reads.checkSuite(run.check_suite_node_id));
  // API head metadata and job headers bind each other. Executed D is the actual
  // fixed-rerun GITHUB_WORKFLOW_SHA + ordinary clean checkout, not renamed X.
  const jobs = await paged(`actions/runs/${run.id}/jobs?filter=all`, "jobs"); jobs.forEach(job => jobHeader(job, run));
  const name = role === "image" ? "Repackage originals (" + process.env.DOCKER_PLATFORM + ")" :
    role === "source" ? SOURCE_JOB : role === "arm-originals" ? ARM_JOB : "Publish complete multi-arch manifest";
  const current = jobs.filter(job => job.name === name && job.run_attempt === run.run_attempt); assert.equal(current.length, 1); assert.equal(current[0].status, "in_progress");
  return { get, paged, run, jobs, current: current[0] };
}
function sourceLine(state, consumerRun) {
  if (sourceFrames.has(state)) {
    const held = sourceFrames.get(state), value = { ...held, data: held.mode === "full" ? held.data : null };
    assert.equal(value.selection.consumerRun, consumerRun);
    const line = SOURCE_PREFIX + Buffer.from(core.canonicalJson(value)).toString("base64"); parseSourceFrame("2000-01-01T00:00:00Z " + line); return line;
  }
  return SOURCE_PREFIX + Buffer.from(core.canonicalJson({ ...selected(state.facts), consumerRun, controller: state.controller })).toString("base64");
}
function sourceLocator(log, run, physical) {
  assert.ok(typeof log === "string" && Buffer.byteLength(log) <= 64 * 1024 * 1024);
  const lines = log.split(/\r?\n/).filter(line => /^\d{4}-\d\d-\d\dT\S+Z /.test(line) && line.slice(line.indexOf(" ") + 1).startsWith(SOURCE_PREFIX)); assert.equal(lines.length, 1, "Missing/ambiguous original consumer source locator");
  passed(step(physical, SOURCE_STEP)); const time = Date.parse(lines[0].split(" ")[0]), selectedStep = step(physical, SOURCE_STEP);
  const second = value => Math.floor(Date.parse(value) / 1000);
  assert.ok(Number.isFinite(time) && second(selectedStep.started_at) <= Math.floor(time / 1000) && Math.floor(time / 1000) <= second(selectedStep.completed_at));
  const encoded = lines[0].slice(lines[0].indexOf(SOURCE_PREFIX) + SOURCE_PREFIX.length); assert.ok(encoded.length <= 87384 && /^[A-Za-z0-9+/]+={0,2}$/.test(encoded));
  const bytes = Buffer.from(encoded, "base64"); let value = JSON.parse(bytes.toString("utf8")); assert.equal(bytes.toString("base64"), encoded); assert.equal(core.canonicalJson(value), bytes.toString("utf8"));
  if (value.kind === SOURCE_KIND) value = parseSourceFrame(log).selection;
  else assert.ok(encoded.length <= 4096, "Oversized legacy locator is not a handoff");
  assert.deepEqual(Object.keys(value).sort(), ["source", "version", "releaseRun", "publisher", "releaseId", "consumerRun", "controller"].sort());
  assert.equal(value.consumerRun, run.id); positive(value.releaseRun); positive(value.releaseId); core.assertVersion(value.version);
  for (const key of ["source", "publisher", "controller"]) assert.match(value[key], SHA); return value;
}
async function crossRunGuard(state, context) {
  const publisher = await context.get("actions/runs/" + state.input.releaseRun); assert.equal(publisher.id, state.input.releaseRun); assert.ok(Number.isFinite(Date.parse(publisher.created_at)));
  const currentTime = Date.parse(context.run.created_at); assert.ok(Number.isFinite(currentTime));
  const runs = await context.paged(`actions/workflows/${context.run.workflow_id}/runs?created=${encodeURIComponent(publisher.created_at + ".." + context.run.created_at)}`, "workflow_runs");
  for (const run of runs.filter(value => value.id !== context.run.id)) {
    const time = Date.parse(run.created_at); assert.ok(Number.isFinite(time));
    if (time > currentTime || time === currentTime && run.id > context.run.id) continue; // queued later consumers cannot become this run's origin
    assert.equal(run.workflow_id, context.run.workflow_id); assert.equal(run.repository?.id, 1164779771); assert.equal(run.path, WORKFLOW);
    const jobs = await context.paged(`actions/runs/${positive(run.id)}/jobs?filter=all`, "jobs"); jobs.forEach(job => jobHeader(job, run));
    if (jobs.length && jobs.every(job => job.status === "completed" && job.conclusion === "skipped")) continue;
    const family = jobs.filter(job => job.name === SOURCE_JOB && source.executionKey(job));
    const origin = family.sort((a,b) => a.run_attempt - b.run_attempt)[0];
    const locator = origin ? sourceLocator(await state.reads.jobLog(origin.id), run, origin) : null;
    assert.ok(!locator || locator.releaseRun !== state.input.releaseRun && locator.releaseId !== state.facts.release.id &&
      !(locator.source === state.facts.source.revision && locator.version === state.facts.release.version), "This formal output already has consumer run " + run.id + "; resume that original run");
    // A foreign self-described locator cannot authorize another assembly. Only
    // an official same-D executed file can prove this is a different release.
    assert.ok(locator && locator.controller === state.controller && run.head_sha === state.controller, "Unknown prior consumer output; restore original run " + run.id);
    consumerSuite(run, await state.reads.checkSuite(run.check_suite_node_id), state.controller);
  }
}
async function freshConsumer(context, phaseStep) {
  const fresh = await context.get("actions/runs/" + context.run.id);
  for (const key of ["id", "run_attempt", "workflow_id", "head_sha", "head_branch", "path", "event", "check_suite_id", "check_suite_node_id"]) assert.deepEqual(fresh[key], context.run[key], "Consumer changed before registry write");
  assert.equal(fresh.status, "in_progress"); const job = await context.get("actions/jobs/" + context.current.id); jobHeader(job, fresh);
  assert.equal(job.id, context.current.id); assert.equal(job.run_attempt, fresh.run_attempt); assert.equal(job.status, "in_progress"); assert.equal(step(job, phaseStep).status, "in_progress");
}
function imageHistory(jobs, run, platform, beforeAttempt = run.run_attempt + 1, final = false) {
  assert.ok(PLATFORMS.includes(platform)); const name = "Repackage originals (" + platform + ")";
  const family = jobs.filter(job => job.name === name && job.run_attempt < beforeAttempt); family.forEach(job => jobHeader(job, run));
  assert.equal(new Set(family.map(job => job.run_attempt)).size, family.length, "Ambiguous architecture attempt");
  const latest = [...family].sort((a,b) => b.run_attempt - a.run_attempt)[0]; if (final) { assert.ok(latest); passed(latest); }
  const originals = new Map();
  for (const job of family) {
    assert.equal(job.status, "completed", "Prior assembly is active/unknown");
    const build = step(job, BUILD);
    if (build.conclusion === "success") {
      passed(job); for (const name of IMAGE_STEPS) passed(step(job, name));
      const key = native.executionKey(job); if (!originals.has(key) || job.run_attempt < originals.get(key).run_attempt) originals.set(key, job);
    } else {
      for (const name of [SMOKE, PUSH, UPLOAD]) skipped(step(job, name));
      assert.ok(build.status === "completed" && ["failure", "skipped"].includes(build.conclusion), "Unknown/cancelled assembly cannot retry");
      assert.equal(step(job, REUSE).status, "completed"); assert.ok(["success", "failure", "skipped", "cancelled"].includes(step(job, REUSE).conclusion));
      if (step(job, REUSE).conclusion === "success") { passed(job); passed(step(job, SELECT)); skipped(build); }
    }
  }
  assert.ok(originals.size <= 1, "A second physical image assembly cannot replace originals"); const physical = [...originals.values()][0];
  if (family.some(job => step(job, REUSE).conclusion !== "skipped")) assert.ok(physical, "Recorded reuse lost its original physical image; never reassemble");
  if (physical) for (const job of family) if (step(job, BUILD).conclusion !== "success") skipped(step(job, BUILD));
  if (final) assert.ok(physical, "Successful logical image has no original physical carrier");
  if (final && step(latest, BUILD).conclusion !== "success") { passed(step(latest, SELECT)); passed(step(latest, REUSE)); for (const name of IMAGE_STEPS.slice(1)) skipped(step(latest, name)); }
  return { latest, physical };
}
const DIGEST_ZIP_READER = String.raw`import io,stat,sys,zipfile
raw=sys.stdin.buffer.read(65537); assert 0<len(raw)<=65536
with zipfile.ZipFile(io.BytesIO(raw)) as archive:
 items=archive.infolist(); assert len(items)==1
 item=items[0]; assert item.filename==sys.argv[1] and not item.is_dir() and not item.flag_bits&1
 assert stat.S_IFMT(item.external_attr>>16) in (0,stat.S_IFREG) and 0<item.file_size<=4096
 data=archive.read(item); assert len(data)==item.file_size
 sys.stdout.write(data.decode('utf-8'))`;
function decodeCarrier(bytes, filename, facts, platform) {
  return digestRecord(JSON.parse(command("python3", ["-I", "-c", DIGEST_ZIP_READER, filename], { input: bytes, maxBuffer: 4096 })), facts, platform);
}
function physicalUpload(log, job, artifact, runId, uploadName = UPLOAD) {
  assert.ok([UPLOAD, ARM_UPLOAD].includes(uploadName), "Unknown original upload role");
  assert.ok(typeof log === "string" && Buffer.byteLength(log) <= 64 * 1024 * 1024);
  const upload = step(job, uploadName); passed(upload);
  const second = value => Math.floor(Date.parse(value) / 1000), start = second(upload.started_at), end = second(upload.completed_at);
  assert.ok(Number.isSafeInteger(start) && Number.isSafeInteger(end) && start <= end);
  const lines = log.split(/\r?\n/).map(line => line.match(/^(\d{4}-\d\d-\d\dT\S+Z) (.*)$/)).filter(Boolean);
  const escaped = artifact.name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const records = lines.map(line => ({ line, match: line[2].match(new RegExp(`^Artifact ${escaped} has been successfully uploaded! Final size is ([1-9][0-9]*) bytes\\. Artifact ID is ([1-9][0-9]*)$`)) })).filter(value => value.match);
  assert.equal(records.length, 1, "Missing/ambiguous original physical upload record");
  assert.equal(positive(records[0].match[1]), artifact.size_in_bytes); assert.equal(positive(records[0].match[2]), artifact.id);
  const urls = lines.filter(line => line[2].startsWith("Artifact download URL: ")); assert.equal(urls.length, 1, "Missing/ambiguous original download URL");
  assert.equal(urls[0][2], `Artifact download URL: https://github.com/${sidecar.REPOSITORY}/actions/runs/${runId}/artifacts/${artifact.id}`);
  for (const line of [records[0].line, urls[0]]) assert.ok(Number.isSafeInteger(second(line[1])) && start <= second(line[1]) && second(line[1]) <= end, "Upload log is outside the original physical step");
}
async function readCarrier(state, context, platform, physical) {
  const direct = await context.get("actions/jobs/" + physical.id); assert.equal(source.fingerprint(direct), source.fingerprint(physical)); assert.equal(direct.run_attempt, physical.run_attempt);
  const name = `docker-digest-${context.run.id}-${physical.run_attempt}-${PLATFORMS.indexOf(platform)}`;
  const all = await context.paged(`actions/runs/${context.run.id}/artifacts`, "artifacts"), matches = all.filter(value => value.name === name); assert.equal(matches.length, 1, "Original digest carrier missing/ambiguous; never reassemble");
  const artifact = matches[0]; assert.equal(artifact.expired, false); assert.match(artifact.digest, DIGEST); positive(artifact.id);
  assert.ok(Number.isSafeInteger(artifact.size_in_bytes) && artifact.size_in_bytes > 0 && artifact.size_in_bytes <= 65536);
  assert.equal(artifact.workflow_run?.id, context.run.id); assert.equal(artifact.workflow_run.head_sha, context.run.head_sha); assert.equal(artifact.workflow_run.head_branch, context.run.head_branch);
  const upload = step(physical, UPLOAD), second = value => Math.floor(Date.parse(value) / 1000);
  assert.ok(Number.isFinite(second(artifact.created_at)) && second(upload.started_at) <= second(artifact.created_at) && second(artifact.created_at) <= second(upload.completed_at));
  physicalUpload(await state.reads.jobLog(physical.id), physical, artifact, context.run.id);
  const bytes = Buffer.from(await state.reads.artifactBytes(artifact.id, artifact.size_in_bytes)); assert.equal(bytes.length, artifact.size_in_bytes); assert.equal("sha256:" + core.sha256(bytes), artifact.digest);
  const record = decodeCarrier(bytes, digestFile(context.run.id, physical.run_attempt, platform), state.facts, platform);
  const fields = value => [value.id, value.name, value.expired, value.size_in_bytes, value.digest, value.created_at, value.workflow_run];
  assert.deepEqual(fields(await context.get("actions/artifacts/" + artifact.id)), fields(artifact));
  const fresh = await context.get("actions/jobs/" + physical.id); assert.equal(fresh.run_attempt, physical.run_attempt);
  assert.equal(source.fingerprint(fresh), source.fingerprint(physical)); return record;
}
function remoteDigest(record) {
  const value = JSON.parse(command("docker", ["buildx", "imagetools", "inspect", "--format", "{{json .Manifest}}", IMAGE + "@" + record.digest])); assert.equal(value.digest, record.digest);
}
async function noPriorCarrier(context, platform) {
  assert.ok(PLATFORMS.includes(platform)); const pattern = new RegExp("^docker-digest-" + positive(context.run.id) + "-[1-9][0-9]*-" + PLATFORMS.indexOf(platform) + "$");
  const artifacts = await context.paged(`actions/runs/${context.run.id}/artifacts`, "artifacts");
  assert.ok(!artifacts.some(value => pattern.test(value.name)), "An orphan/expired original carrier exists; never reassemble without physical proof");
}
async function selectImage(state, verifyRemote = false) {
  const context = await consumerExecution(state, "image"), platform = process.env.DOCKER_PLATFORM;
  if (!verifyRemote) assert.equal(step(context.current, SELECT).status, "in_progress");
  else { assert.equal(step(context.current, REUSE).status, "in_progress"); for (const name of ["Copy authenticated binary and Host originals", BUILD, SMOKE]) skipped(step(context.current, name)); }
  await crossRunGuard(state, context); const history = imageHistory(context.jobs, context.run, platform, context.run.run_attempt);
  if (!history.physical) { assert.equal(verifyRemote, false, "No original image exists"); await noPriorCarrier(context, platform); output("mode", "build"); return; }
  const record = await readCarrier(state, context, platform, history.physical);
  if (verifyRemote) remoteDigest(record); output("mode", "reuse"); output("digest", record.digest);
}
async function main() {
  const [phase] = process.argv.slice(2); assert.equal(process.argv.length, 3);
  if (phase === "stage") return stage(); if (phase === "smoke") return smoke();
  const state = await authenticated(), facts = state.facts;
  if (phase === "source") {
    const context = await consumerExecution(state, "source"); await crossRunGuard(state, context);
    output("release_run", state.input.releaseRun); output("source", facts.source.revision); output("version", facts.release.version);
    console.log(sourceLine(state, context.run.id)); return;
  }
  if (phase === "select" || phase === "reuse") return selectImage(state, phase === "reuse");
  if (phase === "push") {
    const context = await consumerExecution(state, "image"); for (const name of [SELECT, BUILD, SMOKE]) passed(step(context.current, name));
    await crossRunGuard(state, context);
    const platform = process.env.DOCKER_PLATFORM; target(platform);
    const image = candidateImage(process.env.GITHUB_RUN_ID, process.env.GITHUB_RUN_ATTEMPT, platform); assert.equal(process.env.DOCKER_IMAGE, image);
    await refreshPublication(state);
    await freshConsumer(context, PUSH);
    command("docker", ["push", image], { timeout: 15 * 60000 });
    const references = JSON.parse(command("docker", ["image", "inspect", "--format", "{{json .RepoDigests}}", image]));
    const matching = references.filter(value => value.startsWith(IMAGE + "@")); assert.equal(matching.length, 1);
    const record = digestRecord({ ...selected(facts), platform, digest: matching[0].slice(IMAGE.length + 1) }, facts);
    fs.writeFileSync(path.join(process.env.RUNNER_TEMP, digestFile(process.env.GITHUB_RUN_ID, process.env.GITHUB_RUN_ATTEMPT, platform)), JSON.stringify(record) + "\n", { flag: "wx" }); return;
  }
  assert.equal(phase, "publish"); const context = await consumerExecution(state, "merge"), records = [];
  await crossRunGuard(state, context);
  for (const platform of PLATFORMS) {
    const original = imageHistory(context.jobs, context.run, platform, context.run.run_attempt + 1, true).physical;
    const record = await readCarrier(state, context, platform, original); remoteDigest(record); records.push(record);
  }
  const versionTag = IMAGE + ":v" + facts.release.version;
  await refreshPublication(state);
  const latest = await state.reads.json("releases/latest"), tags = publicationTags(facts, latest);
  await freshConsumer(context, "Recheck source and publish both architectures together");
  command("docker", ["buildx", "imagetools", "create", ...tags.flatMap(tag => ["--tag", tag]), ...records.map(value => IMAGE + "@" + value.digest)], { timeout: 5 * 60000 });
  verifyLatest(latest, await state.reads.json("releases/latest"));
  // Use Buildx's descriptor digest, not a hash of CLI-formatted JSON/newlines.
  const inspect = name => JSON.parse(command("docker", ["buildx", "imagetools", "inspect", "--format", "{{json .Manifest}}", name]));
  const digest = manifestReport(inspect(versionTag), records);
  if (tags.includes(IMAGE + ":latest")) assert.equal(manifestReport(inspect(IMAGE + ":latest"), records), digest);
  output("image", IMAGE + "@" + digest);
}
module.exports = { RUNTIME, IMAGE, PLATFORMS, SELECT, BUILD, SMOKE, PUSH, UPLOAD, REUSE, SOURCE_JOB, SOURCE_STEP, SOURCE_PREFIX, ARM_JOB, ARM_UPLOAD,
  safeEnv, eventRun, locate, target, verifyBytes, original, hostFiles, elfReport, imageReference, candidateImage, digestFile, smokeCommands, digestRecord,
  manifestReport, latestIdentity, publicationTags, verifyLatest, consumerSuite, consumerExecution, freshConsumer, sourceLine, sourceLocator, crossRunGuard, imageHistory, decodeCarrier, readCarrier, noPriorCarrier, authenticated,
  physicalUpload, refreshPublication, selected, step, passed, skipped, jobHeader, parseSourceFrame, readSourceHandoff, freshSourceFacts };
if (require.main === module) main().catch(error => { console.error("Published Docker consumer: " + error.message); process.exitCode = 1; });
