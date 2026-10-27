"use strict";
// Read-only public proof. A completion JSON is data; only an independently
// authenticated, pinned main controller's real successful execution attests it.
const assert = require("node:assert/strict");
const source = require("./frozen-source-admission.cjs");
const core = require("./crate-release.cjs");
const native = require("./native-originals.cjs");
const sidecar = require("./sidecar-release.cjs");
const REPOSITORY_ID = 1164779771;
const PREFIX = "Bamboo whole release completion: ";
const KIND = "bamboo-whole-release-completion/v1";
const ACCEPTED_RELEASE_PRODUCERS = Object.freeze({});
const SHA = /^[0-9a-f]{40}$/, DIGEST = /^[0-9a-f]{64}$/;
const CONTROLLER_HELPERS = Object.freeze([
  "crate-release.cjs", "frozen-source-admission.cjs", "compute-publish-order.py",
  "crate-release-frontend.py", "crate-release-lock.py", "release-cargo-sandbox.cjs",
  "release-cargo-upload.cjs", "release-crate-output.cjs", "release-frontend-policy.cjs",
  "frontend-package.cjs", "lotus-next-artifact.cjs", "sidecar-release.cjs",
  "sidecar-build.cjs", "sidecar-browser-host.py", "native-originals.cjs", "native-originals-zip.py",
].map(name => "scripts/" + name));
const keys = (value, names) => assert.deepEqual(Object.keys(value || {}).sort(), [...names].sort(), "Unexpected proof fields");
const positive = value => { assert.ok(Number.isSafeInteger(value) && value > 0); return value; };
const revision = value => { assert.match(value, SHA); return value; };
const digest = value => core.sha256(Buffer.from(core.canonicalJson(value)));
const same = (a, b) => assert.deepEqual(a, b);
const fields = (value, names) => Object.fromEntries(names.map(name => [name, value[name]]));
const releaseIdentity = value => fields(value, ["id", "tag_name", "target_commitish", "draft", "prerelease", "body", "created_at", "published_at", "immutable"]);
const assetIdentities = values => values.map(value => fields(value, ["id", "name", "state", "size", "digest", "created_at"]));
const prIdentity = value => ({ ...fields(value, ["number", "state", "draft", "merged", "merge_commit_sha"]),
  base: { ref: value.base.ref, sha: value.base.sha, repository: fields(value.base.repo, ["id", "full_name"]) },
  head: { ref: value.head.ref, sha: value.head.sha, repository: fields(value.head.repo, ["id", "full_name"]) } });
function repo(value) { assert.equal(value?.id, REPOSITORY_ID); assert.equal(value.full_name, source.REPOSITORY); }
function complete(value) { assert.equal(value.status, "completed"); assert.equal(value.conclusion, "success"); }
function requiredStep(job, name) {
  const values = job.steps.filter(value => value.name === name);
  assert.equal(values.length, 1, "Missing or duplicate required step"); complete(values[0]);
}
function frame(log) {
  assert.ok(typeof log === "string" && Buffer.byteLength(log) <= 64 * 1024 * 1024, "Missing or oversized finalizer log");
  const rows = log.split(/\r?\n/).filter(line => /^\d{4}-\d\d-\d\dT\S+Z /.test(line) && line.slice(line.indexOf(" ") + 1).startsWith(PREFIX));
  assert.equal(rows.length, 1, "Missing or duplicate whole-release completion");
  const encoded = rows[0].slice(rows[0].indexOf(PREFIX) + PREFIX.length);
  assert.ok(/^[A-Za-z0-9+/]+={0,2}$/.test(encoded) && encoded.length <= 87384);
  const bytes = Buffer.from(encoded, "base64"); assert.ok(bytes.length <= 65536);
  assert.equal(bytes.toString("base64"), encoded);
  const value = JSON.parse(bytes.toString("utf8"));
  assert.equal(core.canonicalJson(value), bytes.toString("utf8"), "Noncanonical completion");
  keys(value, ["schemaVersion", "kind", "publicationAuthorized", "repository", "source", "controller", "release", "frontend", "crates", "native", "jobs"]);
  assert.equal(value.schemaVersion, 1); assert.equal(value.kind, KIND); assert.equal(value.publicationAuthorized, false);
  assert.equal(value.repository?.id, REPOSITORY_ID); assert.equal(value.repository.fullName, source.REPOSITORY);
  return value;
}
function assetReference(value) {
  keys(value, ["assetId", "name", "sha256", "sizeBytes"]); positive(value.assetId);
  assert.ok(typeof value.name === "string" && /^[A-Za-z0-9_.-]{1,180}$/.test(value.name));
  assert.match(value.sha256, DIGEST); positive(value.sizeBytes); assert.ok(value.sizeBytes <= sidecar.MAX_BINARY + 8 * 1024 * 1024);
  return value;
}
function selectJobs(latest, all, run, contract) {
  const names = Object.keys(contract.requiredJobs);
  assert.ok(names.length >= 7 && names.length <= 32 && names.includes(contract.finalizer.jobName));
  for (const target of sidecar.TARGETS) assert.ok(names.includes(native.jobName(target)), "Missing native target owner");
  return names.map(name => {
    const values = latest.filter(job => job.name === name); assert.equal(values.length, 1, "Missing or duplicate required release job");
    const logical = values[0], family = all.filter(job => job.name === name);
    assert.ok(family.length && new Set(family.map(job => job.run_attempt)).size === family.length);
    for (const job of family) {
      assert.equal(job.run_id, run.id); assert.equal(job.head_sha, run.head_sha); assert.equal(job.head_branch, "main");
      positive(job.run_attempt); assert.ok(job.run_attempt <= run.run_attempt);
    }
    const newest = [...family].sort((a, b) => b.run_attempt - a.run_attempt)[0];
    assert.equal(source.fingerprint(logical), source.fingerprint(newest)); assert.equal(logical.run_attempt, newest.run_attempt);
    complete(logical); const execution = native.executionKey(logical);
    const physical = family.filter(job => job.status === "completed" && job.conclusion === "success" && native.executionKey(job) === execution)
      .sort((a, b) => a.run_attempt - b.run_attempt)[0];
    assert.ok(physical && Date.parse(physical.started_at) >= Date.parse(run.created_at));
    const steps = contract.requiredJobs[name]; assert.ok(Array.isArray(steps) && steps.length);
    for (const step of steps) { requiredStep(logical, step); requiredStep(physical, step); }
    return { name, logical, physical };
  });
}
async function assetList(get, id) {
  const result = [], ids = new Set(), names = new Set();
  for (let page = 1; page <= 20; page++) {
    const rows = await get("releases/" + positive(id) + "/assets?per_page=100&page=" + page);
    assert.ok(Array.isArray(rows) && rows.length <= 100);
    for (const row of rows) {
      positive(row.id); assert.ok(!ids.has(row.id) && !names.has(row.name), "Duplicate release asset identity");
      ids.add(row.id); names.add(row.name); result.push(row);
    }
    if (rows.length < 100) return result;
  }
  assert.fail("Release asset pagination limit exceeded");
}
async function verifyPublishedSource(input, reads, policy = {}) {
  keys(input, ["sourceRevision", "featurePr", "ciRun", "releaseRun"]);
  const S = revision(input.sourceRevision); for (const key of ["featurePr", "ciRun", "releaseRun"]) positive(input[key]);
  const contracts = policy.releaseProducers ?? ACCEPTED_RELEASE_PRODUCERS;
  assert.ok(Object.keys(contracts).length, "No accepted whole-release producer; Main proof is inactive");
  for (const name of ["json", "checkSuite", "jobLog", "assetBytes", "registryChecksum"]) assert.equal(typeof reads[name], "function");
  const { get, paged, ancestor, branch, blob } = source.sourceReads(reads, policy);
  const mainBefore = await branch("main", true), run = await get("actions/runs/" + input.releaseRun);
  assert.equal(run.id, input.releaseRun); positive(run.run_attempt); assert.ok(run.run_attempt <= 20);
  repo(run.repository); repo(run.head_repository); complete(run);
  assert.equal(run.event, "workflow_dispatch"); assert.equal(run.head_branch, "main"); assert.equal(run.path, source.CONTROLLER);
  const C = revision(run.head_sha); await ancestor(C, mainBefore);
  const official = source.checkedJSON(await reads.checkSuite(run.check_suite_node_id));
  assert.equal(official.databaseId, run.check_suite_id);
  same(official.repository, { databaseId: REPOSITORY_ID, nameWithOwner: source.REPOSITORY });
  same(official.app, { databaseId: 15368, slug: "github-actions" }); assert.equal(official.commit.oid, C);
  const executed = official.workflowRun;
  assert.equal(executed.databaseId, run.id); assert.equal(executed.event, "workflow_dispatch"); assert.equal(executed.runAttempt, run.run_attempt);
  same(executed.workflow, { databaseId: run.workflow_id, name: run.name });
  same(executed.file, { path: source.CONTROLLER, repositoryName: source.REPOSITORY,
    repositoryFileUrl: "https://github.com/" + source.REPOSITORY + "/blob/" + C + "/" + source.CONTROLLER });
  const definition = await blob(source.CONTROLLER, C), contract = contracts[definition.sha256];
  assert.equal(contract?.complete, true, "Unknown or incomplete whole-release controller");
  assert.equal(contract.workflowId, run.workflow_id); assert.equal(contract.workflowName, run.name);
  const anchor = revision(contract.mainHistoryAnchor); await ancestor(anchor, C); await ancestor(anchor, S);
  const workflow = await get("actions/workflows/" + run.workflow_id);
  assert.equal(workflow.id, run.workflow_id); assert.equal(workflow.path, source.CONTROLLER); assert.equal(workflow.state, "active");
  for (const name of CONTROLLER_HELPERS) assert.match(contract.helpers?.[name] ?? "", DIGEST, "Missing whole controller helper");
  for (const [name, checksum] of Object.entries(contract.helpers)) {
    assert.match(checksum, DIGEST); assert.equal((await blob(name, C)).sha256, checksum, "Controller helper bytes changed");
  }
  const facts = await source.verifyAcceptedDevFacts({ sourceRevision: S, featurePr: input.featurePr, ciRun: input.ciRun }, reads, policy);
  const frozenRef = "release/" + S; assert.equal(await branch(frozenRef), S);
  const latest = await paged("actions/runs/" + run.id + "/jobs?filter=latest", "jobs");
  const all = await paged("actions/runs/" + run.id + "/jobs?filter=all", "jobs");
  const jobs = selectJobs(latest, all, run, contract), attemptCache = new Map();
  for (const { physical } of jobs) {
    const direct = await get("actions/jobs/" + physical.id);
    assert.equal(direct.run_attempt, physical.run_attempt); assert.equal(source.fingerprint(direct), source.fingerprint(physical));
    if (!attemptCache.has(physical.run_attempt)) attemptCache.set(physical.run_attempt,
      await paged("actions/runs/" + run.id + "/attempts/" + physical.run_attempt + "/jobs", "jobs"));
    const matches = attemptCache.get(physical.run_attempt).filter(job => job.id === physical.id);
    assert.equal(matches.length, 1); assert.equal(matches[0].run_attempt, physical.run_attempt);
    assert.equal(source.fingerprint(matches[0]), source.fingerprint(physical));
  }
  const finalizer = jobs.find(job => job.name === contract.finalizer.jobName).physical;
  requiredStep(finalizer, contract.finalizer.stepName);
  const record = frame(await reads.jobLog(finalizer.id));
  same(record.controller, { revision: C, workflowPath: source.CONTROLLER, workflowId: run.workflow_id, runId: run.id,
    attempt: finalizer.run_attempt, checkSuiteId: run.check_suite_id, checkSuiteNodeId: run.check_suite_node_id, finalizerJobId: finalizer.id });
  keys(record.source, ["revision", "tree", "releaseRef", "featurePr", "featureRunId", "sourceDispatchSha256"]);
  assert.equal(record.source.revision, S); assert.equal(record.source.tree, facts.source.tree);
  assert.equal(record.source.releaseRef, frozenRef); assert.equal(record.source.featurePr, input.featurePr); assert.equal(record.source.featureRunId, input.ciRun);
  const projected = jobs.filter(job => job.name !== contract.finalizer.jobName).map(({ name, logical, physical }) => ({ name,
    logical: { id: logical.id, attempt: logical.run_attempt }, physical: { id: physical.id, attempt: physical.run_attempt },
    stepsSha256: core.sha256(Buffer.from(native.executionKey(physical))) }));
  assert.ok(Array.isArray(record.jobs) && record.jobs.length === projected.length);
  assert.equal(new Set(record.jobs.map(job => job.name)).size, projected.length);
  for (const observed of record.jobs) {
    keys(observed, ["name", "logical", "physical", "stepsSha256"]);
    const current = projected.find(job => job.name === observed.name); assert.ok(current);
    same(observed.physical, current.physical); assert.equal(observed.stepsSha256, current.stepsSha256);
    keys(observed.logical, ["id", "attempt"]); positive(observed.logical.id); positive(observed.logical.attempt);
    assert.ok(observed.logical.attempt <= finalizer.run_attempt, "Completion cannot name a future logical job");
    const prior = all.filter(job => job.name === observed.name && job.id === observed.logical.id && job.run_attempt === observed.logical.attempt);
    assert.equal(prior.length, 1, "Original completion logical job disappeared"); complete(prior[0]);
    assert.equal(core.sha256(Buffer.from(native.executionKey(prior[0]))), observed.stepsSha256,
      "Immutable old completion cannot adopt a different execution");
  }
  keys(record.release, ["id", "tag", "version", "targetCommit", "receiptAsset", "receiptSha256"]);
  positive(record.release.id); const version = core.assertVersion(record.release.version);
  assert.equal(record.release.tag, "v" + version); assert.equal(record.release.targetCommit, S);
  const release = await get("releases/" + record.release.id);
  assert.equal(release.id, record.release.id); assert.equal(release.tag_name, "v" + version);
  assert.equal(release.target_commitish, S); assert.equal(release.draft, false); assert.equal(release.prerelease, false);
  const tag = await get("git/ref/tags/v" + version); same(tag.object, { type: "commit", sha: S, url: tag.object.url });
  const assets = await assetList(get, release.id), verified = new Map();
  const readAsset = async (ref, maximum = sidecar.MAX_BINARY + 8 * 1024 * 1024) => {
    assetReference(ref); assert.ok(ref.sizeBytes <= maximum, "Original asset exceeds its role bound"); const matches = assets.filter(asset => asset.id === ref.assetId && asset.name === ref.name);
    assert.equal(matches.length, 1, "Original release asset missing or replaced");
    const asset = matches[0]; assert.equal(asset.state, "uploaded"); assert.equal(asset.size, ref.sizeBytes); assert.equal(asset.digest, "sha256:" + ref.sha256);
    if (verified.has(ref.assetId)) { same(verified.get(ref.assetId), ref); return null; }
    const bytes = await reads.assetBytes(ref.assetId, ref.sizeBytes);
    assert.ok(Buffer.isBuffer(bytes)); assert.equal(bytes.length, ref.sizeBytes); assert.equal(core.sha256(bytes), ref.sha256, "Original asset bytes changed");
    verified.set(ref.assetId, ref); return bytes;
  };
  assert.equal(record.release.receiptAsset.name, "release-provenance.json");
  const receipt = JSON.parse((await readAsset(record.release.receiptAsset, 65536)).toString("utf8"));
  assert.equal(digest(receipt), record.release.receiptSha256); same(core.readBodyReceipt(release), receipt);
  const wrapper = JSON.parse((await blob(source.INPUT_PATHS.frontendManifest, S)).bytes);
  const identity = { repository: source.REPOSITORY, sourceRevision: S, frontend: core.frontendIdentity(
    facts.inputs.frontend.packageName, facts.inputs.frontend.packageVersion, wrapper, facts.inputs.frontend) };
  assert.ok(Array.isArray(record.crates) && record.crates.length && record.crates.length <= 128);
  const crateNames = record.crates.map(crate => crate.name); assert.equal(new Set(crateNames).size, crateNames.length); assert.ok(crateNames.includes("bamboo-agent"));
  core.validateReceipt(receipt, identity, crateNames, version); assert.equal(receipt.completed, true);
  keys(receipt.authentication, ["schemaVersion", "algorithm", "mac"]);
  assert.equal(receipt.authentication.schemaVersion, 1); assert.equal(receipt.authentication.algorithm, "hmac-sha256"); assert.match(receipt.authentication.mac, DIGEST);
  // Main has no HMAC key: the independently authenticated C finalizer attests
  // verification. Compare the same immutable public inputs rather than signing.
  const marker = receipt.sourceDispatch, stableFacts = structuredClone(facts);
  delete stableFacts.source.devObservedBefore; delete stableFacts.source.devObservedAfter;
  stableFacts.kind = "bamboo-frozen-source-context/v2";
  stableFacts.source.releaseRef = frozenRef; stableFacts.source.mainHistoryAnchor = anchor;
  stableFacts.controller = { revision: C, workflowPath: source.CONTROLLER, event: "workflow_dispatch", ref: "refs/heads/main" };
  same(marker, core.sourceDispatchContract(stableFacts, String(run.id)));
  assert.equal(digest(marker), record.source.sourceDispatchSha256);
  same(receipt.frontendBytes, { manifestSha256: facts.inputs.frontendManifest.sha256, archiveSha256: facts.inputs.frontendZip.sha256 });
  assert.equal(record.frontend.length, 2);
  for (const [index, name] of ["frontend-manifest.json", "lotus-frontend.zip"].entries()) {
    const ref = record.frontend[index]; assert.equal(ref.name, name);
    assert.equal(ref.sha256, index ? facts.inputs.frontendZip.sha256 : facts.inputs.frontendManifest.sha256); await readAsset(ref, 16 * 1024 * 1024);
  }
  const crateJob = jobs.find(job => job.name === contract.crateJobName);
  assert.ok(crateJob, "Missing original crate owner");
  for (const crate of record.crates) {
    keys(crate, ["name", "archiveSha256", "zipAssetRef", "origin", "verificationSha256", "registryChecksum"]);
    assert.match(crate.name, /^[A-Za-z0-9_-]{1,64}$/); assert.match(crate.archiveSha256, DIGEST); assert.match(crate.verificationSha256, DIGEST);
    assert.equal(crate.registryChecksum, crate.archiveSha256); assert.equal(receipt.packageChecksums[crate.name], crate.archiveSha256);
    keys(crate.origin, ["jobId", "attempt"]); positive(crate.origin.jobId); positive(crate.origin.attempt);
    const origins = all.filter(job => job.id === crate.origin.jobId && job.run_attempt === crate.origin.attempt);
    assert.equal(origins.length, 1, "Original crate producer disappeared");
    const original = origins[0]; assert.equal(original.name, contract.crateJobName); assert.equal(original.run_id, run.id);
    assert.equal(original.head_sha, C); assert.equal(original.head_branch, "main"); assert.equal(original.status, "completed");
    assert.ok(["success", "failure", "cancelled", "timed_out"].includes(original.conclusion));
    assert.ok(Date.parse(original.started_at) >= Date.parse(run.created_at) && Date.parse(original.completed_at) >= Date.parse(original.started_at)
      && Date.parse(original.completed_at) <= Date.parse(finalizer.started_at));
    const direct = await get("actions/jobs/" + original.id);
    assert.equal(direct.run_attempt, original.run_attempt); assert.equal(source.fingerprint(direct), source.fingerprint(original));
    // A failed whole crate worker may already have retained this verified
    // archive. The successful pinned finalizer attests its sealed ZIP; Main
    // must preserve that original job rather than invent a later producer.
    assert.equal(await reads.registryChecksum(crate.name, version), crate.archiveSha256, "Registry original checksum differs");
    assert.equal(crate.zipAssetRef.name, "bamboo-verified-crate-" + crate.name + "-" + version + ".zip");
    await readAsset(crate.zipAssetRef, 64 * 1024 * 1024 + 66560);
  }
  keys(record.native, ["manifestSha256", "assets", "originals", "hostCheckpointRef"]);
  assert.equal(record.native.assets.length, 6);
  const names = [...sidecar.TARGETS.map(sidecar.artifactName), sidecar.HOST_ASSET, sidecar.PROVENANCE, sidecar.MANIFEST];
  same(record.native.assets.map(asset => asset.name), names);
  let manifest;
  for (const ref of record.native.assets) {
    const maximum = ref.name === sidecar.HOST_ASSET ? sidecar.MAX_HOST :
      [sidecar.MANIFEST, sidecar.PROVENANCE].includes(ref.name) ? 1024 * 1024 : sidecar.MAX_BINARY;
    const bytes = await readAsset(ref, maximum);
    if (ref.name === sidecar.MANIFEST) { assert.equal(ref.sha256, record.native.manifestSha256); manifest = JSON.parse(bytes); }
  }
  sidecar.validateManifestV2(manifest, { sourceRevision: S, version }, sidecar.TARGETS.map(target => {
    const ref = record.native.assets.find(asset => asset.name === sidecar.artifactName(target));
    return { target, assetName: ref.name, sha256: ref.sha256, sizeBytes: ref.sizeBytes };
  }));
  const hostAsset = record.native.assets.find(asset => asset.name === sidecar.HOST_ASSET);
  same(fields(manifest.browserHost, ["assetName", "sha256", "sizeBytes"]),
    { assetName: hostAsset.name, sha256: hostAsset.sha256, sizeBytes: hostAsset.sizeBytes });
  same(record.native.originals.map(value => value.target), sidecar.TARGETS);
  for (const original of record.native.originals) {
    keys(original, ["target", "zipAssetRef", "checkpointRef"]);
    assert.equal(original.zipAssetRef.name, native.archiveName(original.target));
    assert.equal(original.checkpointRef.name, native.checkpointName(original.target));
    await readAsset(original.zipAssetRef); await readAsset(original.checkpointRef, 65536);
  }
  assert.equal(record.native.hostCheckpointRef.name, native.HOST_CHECKPOINT);
  await readAsset(record.native.hostCheckpointRef, 65536);
  // Re-read every mutable locator after all original-byte downloads.
  assert.equal(source.runIdentity(await get("actions/runs/" + run.id)), source.runIdentity(run));
  same(source.checkedJSON(await reads.checkSuite(run.check_suite_node_id)), official);
  same(selectJobs(await paged("actions/runs/" + run.id + "/jobs?filter=latest", "jobs"),
    await paged("actions/runs/" + run.id + "/jobs?filter=all", "jobs"), run, contract), jobs);
  same(releaseIdentity(await get("releases/" + release.id)), releaseIdentity(release)); same(await get("git/ref/tags/v" + version), tag);
  same(assetIdentities(await assetList(get, release.id)), assetIdentities(assets)); assert.equal(await branch(frozenRef), S);
  const mainAfter = await branch("main", true); await ancestor(mainBefore, mainAfter);
  return { schemaVersion: 1, kind: "bamboo-published-source-facts/v1", publicationAuthorized: false,
    source: { revision: S, tree: facts.source.tree, releaseRef: frozenRef }, feature: facts.feature,
    controller: record.controller, release: record.release, frontend: record.frontend, native: record.native,
    crates: record.crates, jobs: record.jobs, completionSha256: digest(record), mainObservedBefore: mainBefore, mainObservedAfter: mainAfter };
}
async function treeLeaves(get, tree) {
  const value = await get("git/trees/" + revision(tree) + "?recursive=1");
  assert.equal(value.sha, tree); assert.equal(value.truncated, false); assert.ok(Array.isArray(value.tree) && value.tree.length <= 100000);
  const leaves = new Map();
  for (const entry of value.tree) {
    assert.ok(typeof entry.path === "string" && !entry.path.startsWith("/") && !entry.path.split("/").some(part => ["", ".", ".."].includes(part)));
    if (entry.type === "tree") continue;
    assert.ok(["blob", "commit"].includes(entry.type) && ["100644", "100755", "120000", "160000"].includes(entry.mode));
    assert.equal(entry.type === "commit", entry.mode === "160000"); assert.ok(!leaves.has(entry.path)); revision(entry.sha);
    leaves.set(entry.path, { mode: entry.mode, sha: entry.sha });
  }
  return leaves;
}
async function verifyPromotion(input, reads, policy = {}) {
  keys(input, ["sourceRevision", "featurePr", "ciRun", "releaseRun", "pullRequest", "baseRevision", "headRevision", "snapshotRevision"]);
  const B = revision(input.baseRevision), F = revision(input.headRevision), M = revision(input.snapshotRevision); positive(input.pullRequest);
  const { get, branch, ancestor } = source.sourceReads(reads, policy);
  const pr = await get("pulls/" + input.pullRequest);
  assert.equal(pr.number, input.pullRequest); assert.equal(pr.state, "open"); assert.equal(pr.draft, false);
  repo(pr.base?.repo); repo(pr.head?.repo); assert.equal(pr.base.ref, "main"); assert.equal(pr.base.sha, B); assert.equal(pr.head.sha, F); assert.equal(pr.merge_commit_sha, M);
  assert.equal(await branch("main", true), B, "Promotion base changed");
  const facts = await verifyPublishedSource({ sourceRevision: input.sourceRevision, featurePr: input.featurePr, ciRun: input.ciRun, releaseRun: input.releaseRun }, reads, policy);
  await ancestor(facts.source.revision, F); // Equal trees never establish normal promotion history.
  const commits = await Promise.all([B, F, M].map(sha => get("git/commits/" + sha)));
  for (let i = 0; i < commits.length; i++) assert.equal(commits[i].sha, [B,F,M][i]);
  same(commits[2].parents.map(value => value.sha), [B,F]);
  const [released, base, head, snapshot] = await Promise.all([facts.source.tree, ...commits.map(value => value.tree.sha)].map(tree => treeLeaves(get, tree)));
  const paths = policy.controlPaths;
  assert.ok(Array.isArray(paths) && paths.length && paths.length <= 128 && new Set(paths).size === paths.length);
  for (const path of [source.WORKFLOW, source.CONTROLLER, "scripts/published-source-admission.cjs", ...CONTROLLER_HELPERS]) {
    assert.ok(paths.includes(path), "Incomplete trusted control closure");
    assert.ok(base.has(path) && ["100644", "100755"].includes(base.get(path).mode), "Trusted control must be an ordinary base blob");
  }
  for (const path of paths) assert.ok(path === "AGENTS.md" || path === "CONTRIBUTING.md" || path === "gitflow.md" || /^(scripts|docs|\.github\/workflows)\/[A-Za-z0-9_./-]+$/.test(path) && !path.split("/").includes(".."));
  const overlaid = new Map(released);
  for (const path of paths) { if (base.has(path)) overlaid.set(path, base.get(path)); else overlaid.delete(path); }
  const mapKey = map => core.canonicalJson([...map].sort(([a],[b]) => a.localeCompare(b)));
  // A released feature tree may still carry the old Main workflow. Preserve
  // the whole currently trusted closure; exact S passes only when it already
  // contains that complete closure. No feature-complete => Main-capable leap.
  const releasedKey = mapKey(released), trustedKey = mapKey(overlaid);
  assert.equal(mapKey(head), trustedKey, "Promotion changed product or failed to retain the complete trusted controls");
  assert.equal(mapKey(snapshot), mapKey(head), "Actual promotion merge differs from its reviewed source/control closure");
  same(prIdentity(await get("pulls/" + input.pullRequest)), prIdentity(pr)); assert.equal(await branch("main", true), B);
  return { ...facts, kind: "bamboo-main-promotion-facts/v1", promotion: { pullRequest: input.pullRequest, baseRevision: B, headRevision: F,
    snapshotRevision: M, snapshotTree: commits[2].tree.sha, controlMode: mapKey(head) === releasedKey ? "released-source" : "complete-trusted-base" } };
}
module.exports = { PREFIX, KIND, CONTROLLER_HELPERS, ACCEPTED_RELEASE_PRODUCERS, frame, assetReference, selectJobs, treeLeaves, verifyPublishedSource, verifyPromotion };
