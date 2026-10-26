"use strict";

// Read-only source context, not publication authority or a signed receipt.
// Activation must install a reviewed complete producer in the trusted controller.
const assert = require("node:assert/strict");
const { createHash } = require("node:crypto");
const REPOSITORY = "bigduu/Bamboo-agent";
const REPOSITORY_ID = 1164779771;
const WORKFLOW = ".github/workflows/ci.yml";
const CONTROLLER = ".github/workflows/publish-crate.yml";
const SHA = /^[0-9a-f]{40}$/;
const DIGEST = /^[0-9a-f]{64}$/;
// Same semver boundary as the existing read-only Lotus artifact verifier.
const VERSION = /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*)(?:\.(?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*))*))?(?:\+([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?$/;
const INPUT_PATHS = Object.freeze({
  cargoLock: "Cargo.lock",
  frontendLock: "scripts/frontend-package-lock.json",
  frontendManifest: "crates/app/bamboo-server/frontend_package/frontend-manifest.json",
  frontendZip: "crates/app/bamboo-server/frontend_package/lotus-frontend.zip",
  nativeContract: "scripts/sidecar-release.cjs",
});
const COVERAGE = Object.freeze(["unit", "e2e", "linux-default", "windows-native", "macos-native",
  "portable-node", "lint", "msrv", "docs", "security"]);
const REQUIRED_HELPERS = Object.freeze(["scripts/run-feature-tests.cjs", "scripts/native-test-owner.cjs",
  "scripts/actor-windows-native.ps1"]);
// A339/C0f and the deployed compile-only/v6 routes have no complete v2 admission.
const ACCEPTED_PRODUCERS = Object.freeze({});
const hash = value => createHash("sha256").update(value).digest("hex");
const positive = value => Number.isSafeInteger(value) && value > 0;
const date = value => { assert.ok(typeof value === "string" && Number.isFinite(Date.parse(value)), "Missing timestamp"); return Date.parse(value); };
const exactKeys = (value, keys) => assert.deepEqual(Object.keys(value).sort(), [...keys].sort(), "Unexpected input fields");
function repo(value) {
  assert.equal(value?.id, REPOSITORY_ID, "Foreign repository ID");
  assert.equal(value.full_name, REPOSITORY, "Foreign repository name");
}
function revision(value) { assert.match(value, SHA); return value; }
function checkedJSON(value) {
  assert.ok(value && typeof value === "object" && Buffer.byteLength(JSON.stringify(value)) <= 24 * 1024 * 1024,
    "Missing or oversized API response");
  return value;
}
function fingerprint(job) {
  return JSON.stringify([job.id, job.run_id, job.name, job.head_sha, job.head_branch, job.status, job.conclusion,
    job.started_at, job.completed_at, job.check_run_url, job.steps]);
}
function runIdentity(run) {
  return JSON.stringify([run.id, run.run_attempt, run.workflow_id, run.check_suite_id, run.check_suite_node_id,
    run.event, run.head_sha, run.head_branch, run.status, run.conclusion, run.created_at, run.updated_at]);
}

// The adapter is supplied by trusted controller code: fixed-origin REST GETs,
// the existing read-only check-suite GraphQL query, and original job-log GETs.
// There is deliberately no CLI, downloaded-code execution, token, signer or write adapter.
async function resolveFrozenSource(input, reads, policy = {}) {
  exactKeys(input, ["sourceRevision", "releaseRef", "featurePr", "ciRun"]);
  const S = revision(input.sourceRevision);
  assert.equal(input.releaseRef, `release/${S}`, "Frozen ref must name its exact cut");
  assert.ok(positive(input.featurePr) && positive(input.ciRun), "Exact PR/run locators required");
  const producers = policy.producers ?? ACCEPTED_PRODUCERS;
  assert.ok(Object.keys(producers).length, "No accepted complete v2 feature producer; source foundation is inactive");
  const env = policy.environment ?? process.env;
  const C = revision(env.GITHUB_WORKFLOW_SHA);
  assert.equal(env.GITHUB_SHA, C, "Actual controller SHA must remain separate from product source");
  assert.equal(env.GITHUB_EVENT_NAME, "workflow_dispatch");
  assert.equal(env.GITHUB_REPOSITORY, REPOSITORY);
  assert.equal(env.GITHUB_REF, "refs/heads/main");
  assert.equal(env.GITHUB_REF_PROTECTED, "true");
  assert.equal(env.GITHUB_WORKFLOW_REF, `${REPOSITORY}/${CONTROLLER}@refs/heads/main`);
  const mainHistoryAnchor = revision(policy.mainHistoryAnchor);
  const get = async route => checkedJSON(await reads.json(route));
  const paged = async (route, key) => {
    const values = [], seen = new Set();
    for (let page = 1; page <= 20; page++) {
      const result = await get(`${route}${route.includes("?") ? "&" : "?"}per_page=100&page=${page}`);
      assert.ok(Array.isArray(result[key]) && result[key].length <= 100, "Invalid API page");
      for (const value of result[key]) {
        assert.ok(positive(value.id) && !seen.has(value.id), "Duplicate API identity");
        seen.add(value.id); values.push(value);
      }
      if (result[key].length < 100) return values;
    }
    assert.fail("API pagination limit exceeded");
  };
  const ancestor = async (base, head) => {
    const value = await get(`compare/${revision(base)}...${revision(head)}`);
    assert.ok(["ahead", "identical"].includes(value.status) && value.base_commit?.sha === base &&
      value.merge_base_commit?.sha === base, "Normal ancestry is missing");
  };
  const branch = async (name, protectedBranch = false) => {
    const value = await get(`branches/${encodeURIComponent(name)}`);
    assert.equal(value.name, name);
    if (protectedBranch) assert.equal(value.protected, true, `${name} must remain protected`);
    return revision(value.commit?.sha);
  };
  const mainBefore = await branch("main", true);
  await ancestor(C, mainBefore);
  await ancestor(mainHistoryAnchor, S);
  assert.equal(await branch(input.releaseRef), S, "Frozen branch moved");
  const devBefore = await branch("dev", true);
  await ancestor(S, devBefore);
  const pr = await get(`pulls/${input.featurePr}`);
  assert.equal(pr.number, input.featurePr);
  assert.equal(pr.state, "closed"); assert.equal(pr.merged, true, "Git merge alone is not a merged PR");
  assert.equal(pr.draft, false);
  assert.equal(pr.base?.ref, "dev"); repo(pr.base.repo); repo(pr.head?.repo);
  const F = revision(pr.head.sha), D = revision(pr.merge_commit_sha);
  assert.equal(S, D, "Release must freeze the exact accepted dev merge");
  assert.ok(typeof pr.head.ref === "string" && /^[A-Za-z0-9_./-]+$/.test(pr.head.ref), "Invalid feature branch");
  const mergedAt = date(pr.merged_at);
  const accepted = await get(`git/commits/${D}`);
  assert.equal(accepted.sha, D);
  assert.equal(accepted.parents?.length, 2, "Normal two-parent acceptance required");
  const B = revision(accepted.parents[0].sha), tree = revision(accepted.tree?.sha);
  assert.equal(accepted.parents[1].sha, F, "Accepted head is not the exact PR head");
  // B comes from both immutable merge objects, not a mutable PR base.sha view.
  const run = await get(`actions/runs/${input.ciRun}`);
  assert.equal(run.id, input.ciRun); assert.ok(positive(run.run_attempt) && run.run_attempt <= 20);
  repo(run.repository); repo(run.head_repository);
  assert.equal(run.event, "pull_request"); assert.equal(run.head_sha, F); assert.equal(run.head_branch, pr.head.ref);
  assert.equal(run.name, "CI"); assert.equal(run.path, WORKFLOW);
  assert.equal(run.status, "completed"); assert.equal(run.conclusion, "success");
  assert.ok(date(run.created_at) <= mergedAt);
  assert.ok(positive(run.workflow_id) && positive(run.check_suite_id) && typeof run.check_suite_node_id === "string");
  assert.ok(Array.isArray(run.pull_requests), "Missing PR-association API shape");
  for (const association of run.pull_requests) {
    assert.equal(association.number, pr.number, "Conflicting PR association");
    assert.equal(association.head?.sha, F); assert.equal(association.base?.sha, B);
  }
  const official = checkedJSON(await reads.checkSuite(run.check_suite_node_id));
  assert.equal(official.databaseId, run.check_suite_id);
  assert.deepEqual(official.repository, { databaseId: REPOSITORY_ID, nameWithOwner: REPOSITORY });
  assert.equal(official.app?.databaseId, 15368); assert.equal(official.app.slug, "github-actions");
  assert.equal(official.commit?.oid, F);
  const executed = official.workflowRun;
  assert.equal(executed?.databaseId, run.id); assert.equal(executed.event, run.event);
  assert.equal(executed.runAttempt, run.run_attempt);
  assert.deepEqual(executed.workflow, { databaseId: run.workflow_id, name: "CI" });
  assert.equal(executed.file?.path, WORKFLOW); assert.equal(executed.file.repositoryName, REPOSITORY);
  const prefix = `https://github.com/${REPOSITORY}/blob/`;
  const fileURL = executed.file.repositoryFileUrl;
  assert.ok(typeof fileURL === "string" && fileURL.startsWith(prefix));
  const Q = revision(fileURL.slice(prefix.length).split("/")[0]);
  assert.equal(fileURL, `${prefix}${Q}/${WORKFLOW}`, "Foreign workflow definition");
  const contentCache = new Map();
  const blob = async (path, ref) => {
    assert.ok(/^[A-Za-z0-9_./-]+$/.test(path) && !path.split("/").includes(".."));
    const item = await get(`contents/${path}?ref=${revision(ref)}`);
    assert.equal(item.path, path); revision(item.sha);
    assert.ok(Number.isSafeInteger(item.size) && item.size >= 0 && item.size <= 16 * 1024 * 1024, "Source blob too large");
    if (!contentCache.has(item.sha)) {
      const body = item.encoding === "base64" ? item : await get(`git/blobs/${item.sha}`);
      assert.equal(body.sha, item.sha); assert.equal(body.encoding, "base64");
      assert.ok(typeof body.content === "string" && /^[A-Za-z0-9+/=\r\n]*$/.test(body.content));
      const encoded = body.content.replace(/[\r\n]/g, ""), bytes = Buffer.from(encoded, "base64");
      assert.equal(bytes.toString("base64"), encoded, "Malformed source base64"); assert.equal(bytes.length, item.size);
      const oid = createHash("sha1").update(`blob ${bytes.length}\0`).update(bytes).digest("hex");
      assert.equal(oid, item.sha, "Source bytes do not match Git blob identity"); contentCache.set(oid, bytes);
    }
    const bytes = contentCache.get(item.sha);
    assert.equal(bytes.length, item.size);
    return { path, blob: item.sha, sha256: hash(bytes), bytes };
  };
  const definition = await blob(WORKFLOW, Q), contract = producers[definition.sha256];
  assert.ok(contract?.complete === true, "Unknown or incomplete feature producer");
  assert.equal(contract.workflowId, run.workflow_id);
  assert.deepEqual(contract.coverage, COVERAGE, "Unassigned feature quality owner");
  assert.ok(Array.isArray(contract.requiredJobs) && contract.requiredJobs.length >= 7 && contract.requiredJobs.length <= 64);
  assert.equal(new Set(contract.requiredJobs).size, contract.requiredJobs.length);
  for (const name of ["Validation Source", "Quick Checks", "Test", "E2E Tests",
    "Native Feature (windows-2025)", "Native Feature (macos-latest)"]) assert.ok(contract.requiredJobs.includes(name));
  for (const path of REQUIRED_HELPERS) assert.match(contract.helpers?.[path] ?? "", DIGEST, "Missing pinned producer helper");
  const workflow = await get(`actions/workflows/${run.workflow_id}`);
  assert.equal(workflow.id, run.workflow_id); assert.equal(workflow.path, WORKFLOW); assert.equal(workflow.state, "active");
  const latestRun = async () => {
    const runs = await paged(`actions/workflows/${run.workflow_id}/runs?event=pull_request&head_sha=${F}`, "workflow_runs");
    const candidates = runs.filter(value => value.event === "pull_request" && value.head_sha === F && value.head_branch === pr.head.ref);
    assert.ok(candidates.some(value => value.id === run.id), "Selected run missing from workflow history");
    for (const value of candidates) assert.ok(date(value.created_at) <= date(run.created_at) && value.id <= run.id, "Newer feature run supersedes selected evidence");
  };
  await latestRun();
  const latest = await paged(`actions/runs/${run.id}/jobs?filter=latest`, "jobs");
  const attempts = new Map(), originals = [];
  const steps = (job, names) => {
    assert.equal(job.status, "completed"); assert.equal(job.conclusion, "success");
    assert.ok(Array.isArray(job.steps) && job.steps.length && Array.isArray(names) && names.length);
    for (const step of job.steps) assert.ok(step.status === "completed" && ["success", "skipped"].includes(step.conclusion), "Failed job step");
    for (const name of names) {
      const selected = job.steps.filter(step => step.name === name);
      assert.equal(selected.length, 1, "Missing/duplicate required step");
      assert.equal(selected[0].conclusion, "success", "Required step skipped");
    }
  };
  for (const name of contract.requiredJobs) {
    const matches = latest.filter(job => job.name === name); assert.equal(matches.length, 1, `Missing/duplicate feature job: ${name}`);
    const logical = matches[0], physical = await get(`actions/jobs/${logical.id}`);
    assert.equal(fingerprint(logical), fingerprint(physical), "Logical job differs from original physical execution");
    assert.equal(physical.run_id, run.id); assert.equal(physical.head_sha, F); assert.equal(physical.head_branch, pr.head.ref);
    assert.ok(positive(physical.run_attempt) && positive(logical.run_attempt) &&
      physical.run_attempt <= logical.run_attempt && logical.run_attempt <= run.run_attempt);
    steps(physical, contract.requiredSteps?.[name]);
    assert.ok(date(physical.started_at) >= date(run.created_at) && date(physical.completed_at) >= date(physical.started_at)
      && date(physical.completed_at) <= mergedAt, "Required execution was not complete before dev admission");
    if (!attempts.has(physical.run_attempt)) attempts.set(physical.run_attempt,
      await paged(`actions/runs/${run.id}/attempts/${physical.run_attempt}/jobs`, "jobs"));
    const members = attempts.get(physical.run_attempt).filter(job => job.id === physical.id);
    assert.equal(members.length, 1, "Missing original physical attempt");
    assert.equal(members[0].run_attempt, physical.run_attempt);
    assert.equal(fingerprint(members[0]), fingerprint(physical), "Original physical attempt changed");
    originals.push({ logical, physical });
  }
  const sourceJob = originals.find(value => value.physical.name === "Validation Source").physical;
  const log = await reads.jobLog(sourceJob.id);
  assert.ok(typeof log === "string" && Buffer.byteLength(log) <= 262144, "Missing or oversized original source log");
  const records = log.split(/\r?\n/).filter(line => /^\d{4}-\d\d-\d\dT\S+Z Event source:/.test(line));
  assert.equal(records.length, 1, "Missing/duplicate original source snapshot");
  const record = records[0].match(/^\d{4}-\d\d-\d\dT\S+Z Event source: ([0-9a-f]{40}); full validation: false$/);
  assert.ok(record, "Release/full validation is not feature acceptance");
  const M = record[1], snapshot = await get(`git/commits/${M}`);
  assert.equal(snapshot.sha, M); assert.deepEqual(snapshot.parents?.map(parent => parent.sha), [B, F]);
  assert.equal(snapshot.tree?.sha, tree, "Actual tested merge tree differs from accepted source");
  assert.ok(Q === F || Q === M, "Workflow definition is not the original feature source");
  assert.equal((await blob(WORKFLOW, M)).sha256, definition.sha256);
  assert.equal((await blob(WORKFLOW, S)).sha256, definition.sha256);
  for (const [path, digest] of Object.entries(contract.helpers)) {
    assert.match(digest, DIGEST);
    assert.equal((await blob(path, M)).sha256, digest, "Tested producer helper changed");
    assert.equal((await blob(path, S)).sha256, digest, "Accepted producer helper changed");
  }
  const inputs = {};
  for (const [name, path] of Object.entries(INPUT_PATHS)) {
    const tested = await blob(path, M), source = await blob(path, S);
    assert.equal(source.blob, tested.blob, "Build input drift from tested source");
    inputs[name] = source;
  }
  const frontend = JSON.parse(inputs.frontendLock.bytes.toString("utf8"));
  exactKeys(frontend, ["schemaVersion", "packageName", "packageVersion", "sourceRevision", "sourceDirty", "entrypoint", "resourcesSha256", "manifestSha256"]);
  assert.equal(frontend.schemaVersion, 1); assert.equal(frontend.packageName, "@bigduu/lotus-next");
  assert.match(frontend.packageVersion, VERSION);
  revision(frontend.sourceRevision); assert.equal(frontend.sourceDirty, false); assert.equal(frontend.entrypoint, "index.html");
  for (const name of ["resourcesSha256", "manifestSha256"]) assert.match(frontend[name], DIGEST);
  const wrapper = JSON.parse(inputs.frontendManifest.bytes.toString("utf8"));
  assert.equal(wrapper.schema_version, 1); assert.equal(wrapper.frontend_name, "lotus-next");
  assert.equal(wrapper.frontend_version, frontend.packageVersion); assert.equal(wrapper.entry, "index.html");
  assert.equal(wrapper.bundle_hash, `sha256:${inputs.frontendZip.sha256}`, "Committed frontend ZIP digest differs");
  const markers = [...inputs.nativeContract.bytes.toString("utf8").matchAll(/^\/\/ BAMBOO_SHARED_EXECUTABLES_V([0-9]+)$/gm)];
  assert.equal(markers.length, 1); assert.equal(markers[0][1], "2", "Frozen source does not select the three-target contract");
  for (const value of Object.values(inputs)) delete value.bytes;
  inputs.frontend = frontend;
  inputs.nativeTargets = ["aarch64-apple-darwin", "x86_64-pc-windows-msvc", "x86_64-unknown-linux-gnu"];
  const freshRun = await get(`actions/runs/${run.id}`);
  assert.equal(runIdentity(freshRun), runIdentity(run), "Feature run changed during proof");
  repo(freshRun.repository); repo(freshRun.head_repository);
  assert.deepEqual(checkedJSON(await reads.checkSuite(run.check_suite_node_id)), official, "Official check-suite changed during proof");
  await latestRun();
  const freshJobs = await paged(`actions/runs/${run.id}/jobs?filter=latest`, "jobs");
  for (const { logical, physical } of originals) {
    const fresh = freshJobs.filter(job => job.name === physical.name);
    assert.equal(fresh.length, 1); assert.equal(fingerprint(fresh[0]), fingerprint(logical));
    assert.equal(fresh[0].run_attempt, logical.run_attempt);
    const direct = await get(`actions/jobs/${physical.id}`);
    assert.equal(direct.run_attempt, physical.run_attempt); assert.equal(fingerprint(direct), fingerprint(physical));
  }
  const freshPr = await get(`pulls/${pr.number}`);
  assert.equal(freshPr.merged, true); assert.equal(freshPr.state, "closed"); assert.equal(freshPr.merge_commit_sha, S);
  assert.equal(freshPr.number, pr.number); assert.equal(freshPr.draft, false);
  assert.equal(freshPr.head?.ref, pr.head.ref); assert.equal(freshPr.merged_at, pr.merged_at);
  assert.equal(freshPr.head?.sha, F); assert.equal(freshPr.base?.ref, "dev"); repo(freshPr.head.repo); repo(freshPr.base.repo);
  const devAfter = await branch("dev", true); await ancestor(devBefore, devAfter);
  const mainAfter = await branch("main", true); await ancestor(mainBefore, mainAfter);
  assert.equal(await branch(input.releaseRef), S, "Frozen branch changed during proof");
  return {
    schemaVersion: 2, kind: "bamboo-frozen-source-context/v2", publicationAuthorized: false,
    source: { releaseRef: input.releaseRef, revision: S, tree, acceptedMerge: D, mainHistoryAnchor, devObservedBefore: devBefore, devObservedAfter: devAfter },
    feature: { pr: pr.number, baseRevision: B, headRevision: F, snapshotRevision: M, mergedAt: pr.merged_at,
      runId: run.id, logicalAttempt: run.run_attempt, workflowId: run.workflow_id, checkSuiteId: run.check_suite_id,
      workflowDefinitionRevision: Q, producerDigest: definition.sha256,
      physicalJobs: originals.map(({ logical, physical }) => ({ name: physical.name, jobId: physical.id,
        logicalAttempt: logical.run_attempt, physicalAttempt: physical.run_attempt,
        startedAt: physical.started_at, completedAt: physical.completed_at, stepsFingerprint: hash(fingerprint(physical)) })) },
    controller: { revision: C, workflowPath: CONTROLLER, event: env.GITHUB_EVENT_NAME, ref: env.GITHUB_REF, mainObservedBefore: mainBefore, mainObservedAfter: mainAfter },
    inputs,
  };
}

module.exports = { REPOSITORY, WORKFLOW, CONTROLLER, INPUT_PATHS, COVERAGE, REQUIRED_HELPERS,
  ACCEPTED_PRODUCERS, resolveFrozenSource };
