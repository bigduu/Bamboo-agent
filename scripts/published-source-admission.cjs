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
const MAIN_WORKFLOW = ".github/workflows/main-admission.yml";
const MAIN_WORKFLOW_SHA256 = "9794f9b3ca4398adc4e1da1eb4aca3ef3df05713d2fcc67a8085bbbd13240b5a";
const MAIN_JOB = "Main Admission", MAIN_STEP = "Verify completed publication and exact Main postimage";
const MAIN_PREFIX = "Bamboo Main admission: ", MAIN_KIND = "bamboo-main-admission/v1";
const CONTROLLER_PREFIX = "Bamboo Main controller maintenance: ";
const CONTROLLER_KIND = "bamboo-main-controller-maintenance/v1";
// These are whole ordinary B controls, never a candidate-supplied path policy.
// Legacy automatic Main compilers must be retired by the separate Ops cutover.
const MAIN_CONTROLS = Object.freeze([MAIN_WORKFLOW, source.WORKFLOW, source.CONTROLLER,
  "scripts/main-admission.cjs", "scripts/published-source-admission.cjs", ...CONTROLLER_HELPERS,
  "AGENTS.md", "docs/releases/ci-publication-flow.md", "docs/delivery-workflow.md", "docs/ci-workflow-audit.md",
  // Initial bootstrap prerequisite: literal consumer and retired-route controls.
  // Their implementation/acceptance and the actual Main installation are separate.
  ".github/workflows/docs.yml", "scripts/docs-deploy.cjs", "scripts/docs-deploy-verify.py",
  ".github/workflows/docker-publish.yml", "docker/Dockerfile", "docker/docker-compose.yml", "scripts/docker-from-release.cjs",
  "scripts/docker-arm-original.cjs",
  ".github/workflows/actor-snapshot-windows.yml", ".github/workflows/codeql.yml", ".github/workflows/frontend-acceptance.yml",
  ".github/workflows/release-cargo-isolation.yml", ".github/workflows/ticket-platform-adapter.yml"]);
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
  // Pretty public provenance includes Source and every crate checkpoint. Its
  // producer role is 1 MiB, independently of the 64 KiB completion log frame.
  const receipt = JSON.parse((await readAsset(record.release.receiptAsset, 1024 * 1024)).toString("utf8"));
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
    assert.equal(ref.sha256, index ? facts.inputs.frontendZip.sha256 : facts.inputs.frontendManifest.sha256);
    // New Source admission is narrower than the legacy 256 MiB ZIP carrier.
    await readAsset(ref, (index ? 16 : 1) * 1024 * 1024);
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
function closure(leaves) {
  return MAIN_CONTROLS.map(path => {
    const entry = leaves.get(path);
    assert.ok(entry && ["100644", "100755"].includes(entry.mode), "Missing ordinary trusted control: " + path);
    return { path, ...entry };
  });
}
async function promotionTree(input, facts, reads) {
  const { get, ancestor } = source.sourceReads(reads, {});
  const { baseRevision: B, headRevision: F, snapshotRevision: M } = input;
  await ancestor(facts.source.revision, F);
  const commits = await Promise.all([B, F, M].map(sha => get("git/commits/" + revision(sha))));
  commits.forEach((commit, i) => assert.equal(commit.sha, [B,F,M][i]));
  same(commits[2].parents.map(value => value.sha), [B,F]);
  const [released, base, head, snapshot] = await Promise.all([facts.source.tree, ...commits.map(value => value.tree.sha)].map(tree => treeLeaves(get, tree)));
  const controls = closure(base), overlaid = new Map(released);
  for (const { path, ...entry } of controls) overlaid.set(path, entry);
  const mapKey = map => core.canonicalJson([...map].sort(([a],[b]) => a.localeCompare(b)));
  assert.equal(mapKey(head), mapKey(overlaid), "Promotion changed product or failed to retain the complete trusted controls");
  assert.equal(mapKey(snapshot), mapKey(head), "Actual promotion merge differs from its reviewed source/control closure");
  return { pullRequest: input.pullRequest, baseRevision: B, headRevision: F, snapshotRevision: M,
    snapshotTree: commits[2].tree.sha, controlMode: mapKey(head) === mapKey(released) ? "released-source" : "complete-trusted-base",
    controlsSha256: digest(controls) };
}
async function verifyPromotion(input, reads, policy = {}) {
  keys(input, ["sourceRevision", "featurePr", "ciRun", "releaseRun", "pullRequest", "baseRevision", "headRevision", "snapshotRevision"]);
  const B = revision(input.baseRevision), F = revision(input.headRevision), M = revision(input.snapshotRevision); positive(input.pullRequest);
  assert.equal(policy.controlPaths, undefined, "Caller-selected control paths are forbidden");
  const { get, branch } = source.sourceReads(reads, policy);
  const pr = await get("pulls/" + input.pullRequest);
  assert.equal(pr.number, input.pullRequest); assert.equal(pr.state, "open"); assert.equal(pr.draft, false);
  repo(pr.base?.repo); repo(pr.head?.repo); assert.equal(pr.base.ref, "main"); assert.equal(pr.base.sha, B); assert.equal(pr.head.sha, F); assert.equal(pr.merge_commit_sha, M);
  assert.equal(await branch("main", true), B, "Promotion base changed");
  const facts = await verifyPublishedSource({ sourceRevision: input.sourceRevision, featurePr: input.featurePr, ciRun: input.ciRun, releaseRun: input.releaseRun }, reads, policy);
  const promotion = await promotionTree(input, facts, reads);
  same(prIdentity(await get("pulls/" + input.pullRequest)), prIdentity(pr)); assert.equal(await branch("main", true), B);
  return { ...facts, kind: "bamboo-main-promotion-facts/v1", promotion };
}
async function controllerTree(input, facts, reads) {
  const { get, ancestor, blob } = source.sourceReads(reads, {});
  const { baseRevision: B, headRevision: F, snapshotRevision: M } = input;
  await ancestor(B, F); // A control branch inherits B, not unreleased D product history.
  const commits = await Promise.all([B,F,M].map(sha => get("git/commits/" + revision(sha))));
  commits.forEach((commit,i) => assert.equal(commit.sha, [B,F,M][i]));
  same(commits[2].parents.map(parent => parent.sha), [B,F]);
  const [base, controls, head, snapshot] = await Promise.all([commits[0].tree.sha, facts.source.tree, commits[1].tree.sha, commits[2].tree.sha].map(tree => treeLeaves(get, tree)));
  closure(base); const selected = closure(controls), expected = new Map(base);
  for (const { path, ...entry } of selected) expected.set(path, entry);
  const key = map => core.canonicalJson([...map].sort(([a],[b]) => a.localeCompare(b)));
  assert.equal(key(head), key(expected), "Controller maintenance changed stable product/lock/mode or copied partial controls");
  assert.equal(key(snapshot), key(head), "Controller merge differs from the exact reviewed postimage");
  // This bounded maintenance contract keeps the known entrypoint/frame version.
  // New Main workflow versions require a separate explicit compatibility slice.
  for (const ref of [B, facts.source.revision]) assert.equal((await blob(MAIN_WORKFLOW, ref)).sha256, MAIN_WORKFLOW_SHA256, "Unknown Main entrypoint; no helper fallback");
  return { pullRequest: input.pullRequest, baseRevision: B, headRevision: F, snapshotRevision: M,
    snapshotTree: commits[2].tree.sha, baseProductTree: commits[0].tree.sha, controlsSha256: digest(selected) };
}
async function verifyControllerMaintenance(input, reads, policy = {}) {
  keys(input, ["sourceRevision", "featurePr", "ciRun", "pullRequest", "baseRevision", "headRevision", "snapshotRevision"]);
  for (const field of ["sourceRevision", "baseRevision", "headRevision", "snapshotRevision"]) revision(input[field]);
  for (const field of ["featurePr", "ciRun", "pullRequest"]) positive(input[field]);
  assert.equal(policy.controlPaths, undefined, "Caller-selected control paths are forbidden");
  const { get, ancestor } = source.sourceReads(reads, policy), B = input.baseRevision;
  assert.equal(await mainAuthority(get), B);
  const pr = await get("pulls/" + input.pullRequest);
  assert.equal(pr.number, input.pullRequest); assert.equal(pr.state, "open"); assert.equal(pr.merged, false); assert.equal(pr.draft, false); assert.equal(pr.mergeable, true);
  repo(pr.base.repo); repo(pr.head.repo); assert.equal(pr.base.ref, "main"); assert.equal(pr.base.sha, B);
  assert.equal(pr.head.sha, input.headRevision); assert.equal(pr.merge_commit_sha, input.snapshotRevision);
  assert.match(pr.head.ref, /^[A-Za-z0-9_./-]{1,255}$/);
  const selected = { sourceRevision: input.sourceRevision, featurePr: input.featurePr, ciRun: input.ciRun };
  const locator = { mode: "controller", ...selected }; same(mainLocator(pr.body), locator);
  // Both reads use the real verifier. Only immutable original logs are memoized;
  // mutable run/jobs/PR/protection/refs are freshly checked after tree reads.
  const logs = new Map(), cached = { ...reads, jobLog: async id => {
    if (!logs.has(id)) logs.set(id, Promise.resolve(reads.jobLog(id))); return logs.get(id);
  } };
  const facts = await source.verifyAcceptedDevFacts(selected, cached, policy);
  const maintenance = await controllerTree(input, facts, reads);
  const fresh = await source.verifyAcceptedDevFacts(selected, cached, policy);
  const stable = value => { const copy = structuredClone(value); delete copy.source.devObservedBefore; delete copy.source.devObservedAfter; return copy; };
  same(stable(fresh), stable(facts), "Controller Source facts changed during admission");
  await ancestor(facts.source.devObservedAfter, fresh.source.devObservedBefore);
  const current = await get("pulls/" + input.pullRequest);
  same(prIdentity(current), prIdentity(pr)); assert.equal(current.mergeable, true); same(mainLocator(current.body), locator);
  assert.equal(await mainAuthority(get), B);
  return { schemaVersion: 1, kind: CONTROLLER_KIND, publicationAuthorized: false,
    controllerSource: { revision: facts.source.revision, tree: facts.source.tree, featurePr: selected.featurePr, ciRun: selected.ciRun,
      featureSnapshotRevision: facts.feature.snapshotRevision, sourceFactsSha256: digest(stable(facts)) }, maintenance };
}
function mainLocator(body) {
  assert.ok(typeof body === "string" && Buffer.byteLength(body) <= 128 * 1024);
  const matches = [...body.matchAll(/<!-- bamboo-main-locator\/v1\n([\s\S]*?)\n-->/g)];
  assert.equal(matches.length, 1, "Require one Main locator; PR text is data only");
  const value = JSON.parse(matches[0][1]);
  assert.ok(["promotion", "bootstrap", "controller"].includes(value.mode));
  keys(value, ["mode", "sourceRevision", "featurePr", "ciRun", ...(value.mode === "promotion" ? ["releaseRun"] : [])]);
  revision(value.sourceRevision); positive(value.featurePr); positive(value.ciRun);
  if (value.mode === "promotion") positive(value.releaseRun);
  return value;
}
async function mainAuthority(get) {
  const repository = await get(""); repo(repository);
  assert.equal(repository.default_branch, "main", "STOP: separate Ops must select default Main");
  const branch = await get("branches/main"); assert.equal(branch.name, "main"); assert.equal(branch.protected, true);
  const checks = branch.protection?.required_status_checks;
  assert.equal(checks?.enforcement_level, "everyone", "Main admission must apply to everyone");
  assert.ok(checks.contexts?.includes(MAIN_JOB));
  same(checks.checks.filter(check => check.context === MAIN_JOB), [{ context: MAIN_JOB, app_id: 15368 }]);
  return revision(branch.commit?.sha);
}
function canonicalLogFrame(log, prefix) {
  assert.ok(typeof log === "string" && Buffer.byteLength(log) <= 64 * 1024 * 1024);
  const rows = log.split(/\r?\n/).filter(line => /^\d{4}-\d\d-\d\dT\S+Z /.test(line) && line.slice(line.indexOf(" ") + 1).startsWith(prefix));
  assert.equal(rows.length, 1, "Missing or duplicate physical Main admission frame");
  const encoded = rows[0].slice(rows[0].indexOf(prefix) + prefix.length);
  assert.ok(/^[A-Za-z0-9+/]+={0,2}$/.test(encoded) && encoded.length <= 87384);
  const bytes = Buffer.from(encoded, "base64"); assert.ok(bytes.length <= 65536); assert.equal(bytes.toString("base64"), encoded);
  const value = JSON.parse(bytes.toString("utf8")); assert.equal(core.canonicalJson(value), bytes.toString("utf8"));
  return value;
}
function logFrame(log) {
  const value = canonicalLogFrame(log, MAIN_PREFIX);
  keys(value, ["schemaVersion", "kind", "publicationAuthorized", "repository", "locatorSha256", "source", "release", "promotion", "execution"]);
  assert.equal(value.schemaVersion, 1); assert.equal(value.kind, MAIN_KIND); assert.equal(value.publicationAuthorized, false);
  same(value.repository, { id: REPOSITORY_ID, fullName: source.REPOSITORY }); assert.match(value.locatorSha256, DIGEST);
  keys(value.source, ["revision", "tree"]); Object.values(value.source).forEach(revision);
  keys(value.release, ["runId", "completionSha256"]); positive(value.release.runId); assert.match(value.release.completionSha256, DIGEST);
  keys(value.promotion, ["pullRequest", "baseRevision", "headRevision", "snapshotRevision", "snapshotTree", "controlMode", "controlsSha256"]);
  positive(value.promotion.pullRequest);
  for (const key of ["baseRevision", "headRevision", "snapshotRevision", "snapshotTree"]) revision(value.promotion[key]);
  assert.ok(["released-source", "complete-trusted-base"].includes(value.promotion.controlMode)); assert.match(value.promotion.controlsSha256, DIGEST);
  executionFields(value.execution);
  return value;
}
function executionFields(value) {
  keys(value, ["revision", "metadataRevision", "workflowPath", "workflowId", "runId", "physicalAttempt", "jobId", "checkSuiteId", "checkSuiteNodeId"]);
  revision(value.revision); revision(value.metadataRevision); assert.equal(value.workflowPath, MAIN_WORKFLOW);
  for (const key of ["workflowId", "runId", "physicalAttempt", "jobId", "checkSuiteId"]) positive(value[key]);
  assert.match(value.checkSuiteNodeId, /^[A-Za-z0-9_=-]{1,128}$/);
}
function controllerLogFrame(log) {
  const value = canonicalLogFrame(log, CONTROLLER_PREFIX);
  keys(value, ["schemaVersion", "kind", "publicationAuthorized", "controllerSource", "maintenance", "repository", "locatorSha256", "execution"]);
  assert.equal(value.schemaVersion, 1); assert.equal(value.kind, CONTROLLER_KIND); assert.equal(value.publicationAuthorized, false);
  same(value.repository, { id: REPOSITORY_ID, fullName: source.REPOSITORY }); assert.match(value.locatorSha256, DIGEST);
  keys(value.controllerSource, ["revision", "tree", "featurePr", "ciRun", "featureSnapshotRevision", "sourceFactsSha256"]);
  for (const key of ["revision", "tree", "featureSnapshotRevision"]) revision(value.controllerSource[key]);
  positive(value.controllerSource.featurePr); positive(value.controllerSource.ciRun); assert.match(value.controllerSource.sourceFactsSha256, DIGEST);
  keys(value.maintenance, ["pullRequest", "baseRevision", "headRevision", "snapshotRevision", "snapshotTree", "baseProductTree", "controlsSha256"]);
  positive(value.maintenance.pullRequest);
  for (const key of ["baseRevision", "headRevision", "snapshotRevision", "snapshotTree", "baseProductTree"]) revision(value.maintenance[key]);
  assert.match(value.maintenance.controlsSha256, DIGEST); executionFields(value.execution);
  assert.equal(value.execution.revision, value.maintenance.baseRevision); assert.equal(value.execution.metadataRevision, value.maintenance.headRevision);
  return value;
}
function controllerFrameLine(value) {
  const line = CONTROLLER_PREFIX + Buffer.from(core.canonicalJson(value)).toString("base64");
  controllerLogFrame("2000-01-01T00:00:00.000Z " + line); return line;
}
function mainFrameLine(value) {
  const line = MAIN_PREFIX + Buffer.from(core.canonicalJson(value)).toString("base64");
  logFrame("2000-01-01T00:00:00.000Z " + line); return line;
}
async function mainSuite(run, reads) {
  const { get } = source.sourceReads(reads, {});
  repo(run.repository); repo(run.head_repository); positive(run.run_attempt);
  assert.ok(run.run_attempt <= 20); assert.equal(run.event, "pull_request_target"); assert.equal(run.path, MAIN_WORKFLOW);
  assert.equal(run.name, "Bamboo Main Admission");
  const official = source.checkedJSON(await reads.checkSuite(run.check_suite_node_id));
  same(official.repository, { databaseId: REPOSITORY_ID, nameWithOwner: source.REPOSITORY });
  same(official.app, { databaseId: 15368, slug: "github-actions" }); assert.equal(official.databaseId, run.check_suite_id);
  // Observed PRT API metadata (including file URL) names F, NOT executed B.
  assert.equal(official.commit.oid, run.head_sha);
  same(official.workflowRun, { databaseId: run.id, event: "pull_request_target", runAttempt: run.run_attempt,
    workflow: { databaseId: run.workflow_id, name: run.name }, file: { path: MAIN_WORKFLOW, repositoryName: source.REPOSITORY,
      repositoryFileUrl: "https://github.com/" + source.REPOSITORY + "/blob/" + run.head_sha + "/" + MAIN_WORKFLOW } });
  const workflow = await get("actions/workflows/main-admission.yml");
  assert.equal(workflow.id, run.workflow_id); assert.equal(workflow.path, MAIN_WORKFLOW); assert.equal(workflow.state, "active");
  return official;
}
async function mainExecution(run, reads) {
  const { get, paged } = source.sourceReads(reads, {});
  complete(run); const official = await mainSuite(run, reads);
  const latest = await paged("actions/runs/" + run.id + "/jobs?filter=latest", "jobs");
  const all = await paged("actions/runs/" + run.id + "/jobs?filter=all", "jobs");
  assert.equal(latest.length, 1); const logical = latest[0]; assert.ok(all.length && all.length <= 20);
  assert.equal(new Set(all.map(job => job.run_attempt)).size, all.length);
  for (const job of all) {
    assert.equal(job.name, MAIN_JOB); assert.equal(job.run_id, run.id); assert.equal(job.head_sha, run.head_sha);
    assert.equal(job.head_branch, run.head_branch); positive(job.run_attempt); assert.ok(job.run_attempt <= run.run_attempt);
  }
  const newest = [...all].sort((a,b) => b.run_attempt - a.run_attempt)[0];
  assert.equal(source.fingerprint(logical), source.fingerprint(newest)); complete(logical); requiredStep(logical, MAIN_STEP);
  const execution = source.executionKey(logical); assert.ok(execution);
  const physical = all.filter(job => source.executionKey(job) === execution).sort((a,b) => a.run_attempt - b.run_attempt)[0];
  requiredStep(physical, MAIN_STEP); assert.ok(Date.parse(physical.started_at) >= Date.parse(run.created_at));
  const direct = await get("actions/jobs/" + physical.id);
  assert.equal(direct.run_attempt, physical.run_attempt); assert.equal(source.fingerprint(direct), source.fingerprint(physical));
  const attempt = await paged("actions/runs/" + run.id + "/attempts/" + physical.run_attempt + "/jobs", "jobs");
  assert.equal(attempt.length, 1); assert.equal(attempt[0].run_attempt, physical.run_attempt); assert.equal(source.fingerprint(attempt[0]), source.fingerprint(physical));
  return { official, logical, physical };
}
async function verifyMergedPromotion(input, reads, policy = {}) {
  keys(input, ["pullRequest", "admissionRun"]); positive(input.pullRequest); positive(input.admissionRun);
  assert.equal(policy.controlPaths, undefined, "Caller-selected control paths are forbidden");
  const { get, ancestor, blob } = source.sourceReads(reads, policy), before = await mainAuthority(get);
  const pr = await get("pulls/" + input.pullRequest);
  assert.equal(pr.number, input.pullRequest); assert.equal(pr.state, "closed"); assert.equal(pr.merged, true); assert.equal(pr.draft, false);
  repo(pr.base.repo); repo(pr.head.repo); assert.equal(pr.base.ref, "main");
  assert.match(pr.head.ref, /^[A-Za-z0-9_./-]{1,255}$/);
  const P = revision(pr.merge_commit_sha), merged = await get("git/commits/" + P);
  assert.equal(merged.sha, P); assert.equal(merged.parents.length, 2);
  const B = revision(merged.parents[0].sha), F = revision(merged.parents[1].sha); assert.equal(pr.head.sha, F); await ancestor(P, before);
  const run = await get("actions/runs/" + input.admissionRun); assert.equal(run.id, input.admissionRun);
  assert.equal(run.head_sha, F); assert.equal(run.head_branch, pr.head.ref);
  const selected = await mainExecution(run, reads), record = logFrame(await reads.jobLog(selected.physical.id));
  assert.ok(Number.isFinite(Date.parse(pr.merged_at)) && Date.parse(selected.physical.completed_at) <= Date.parse(pr.merged_at), "Admission must complete no later than recorded promotion");
  same(record.execution, { revision: B, metadataRevision: F, workflowPath: MAIN_WORKFLOW, workflowId: run.workflow_id,
    runId: run.id, physicalAttempt: selected.physical.run_attempt, jobId: selected.physical.id,
    checkSuiteId: run.check_suite_id, checkSuiteNodeId: run.check_suite_node_id });
  assert.equal((await blob(MAIN_WORKFLOW, B)).sha256, MAIN_WORKFLOW_SHA256, "Unreviewed Main workflow definition at B");
  const locator = mainLocator(pr.body); assert.equal(locator.mode, "promotion", "Bootstrap never authorizes a consumer");
  assert.equal(digest(locator), record.locatorSha256);
  const { mode, ...releaseInput } = locator;
  const facts = await verifyPublishedSource(releaseInput, reads, policy);
  same(record.source, facts.source && { revision: facts.source.revision, tree: facts.source.tree });
  same(record.release, { runId: locator.releaseRun, completionSha256: facts.completionSha256 });
  const promotion = await promotionTree({ pullRequest: input.pullRequest, baseRevision: B, headRevision: F,
    snapshotRevision: record.promotion.snapshotRevision }, facts, reads);
  same(promotion, record.promotion); assert.equal(merged.tree.sha, promotion.snapshotTree, "Merged P differs from the checked M postimage");
  const docs = facts.feature.physicalJobs.filter(job => job.name === "Documentation"); assert.equal(docs.length, 1);
  const stablePR = value => { const identity = prIdentity(value); delete identity.base.sha;
    return { ...identity, mergedAt: value.merged_at, locator: mainLocator(value.body) }; };
  // Historical B is P.parents[0], not the closed PR's moving base.sha view.
  same(stablePR(await get("pulls/" + input.pullRequest)), stablePR(pr));
  assert.equal(source.runIdentity(await get("actions/runs/" + run.id)), source.runIdentity(run));
  same(await mainExecution(run, reads), selected); const after = await mainAuthority(get); await ancestor(before, after); await ancestor(P, after);
  return { ...facts, kind: "bamboo-merged-main-promotion/v1", promotion: { ...promotion, mergeRevision: P },
    admission: { ...record.execution, logicalJobId: selected.logical.id, logicalAttempt: selected.logical.run_attempt },
    featureDocs: { kind: "feature-docs-execution-locator/v1", runId: facts.feature.runId, jobId: docs[0].jobId,
      physicalAttempt: docs[0].physicalAttempt, snapshotRevision: facts.feature.snapshotRevision, sourceTree: facts.source.tree,
      producerDigest: facts.feature.producerDigest, artifactName: "feature-docs-" + facts.feature.runId + "-" + docs[0].physicalAttempt } };
}
module.exports = { PREFIX, KIND, CONTROLLER_HELPERS, ACCEPTED_RELEASE_PRODUCERS, frame, assetReference, selectJobs, treeLeaves, releaseIdentity, assetIdentities,
  verifyPublishedSource, verifyPromotion, MAIN_WORKFLOW, MAIN_WORKFLOW_SHA256, MAIN_JOB, MAIN_STEP, MAIN_PREFIX, MAIN_KIND,
  MAIN_CONTROLS, closure, promotionTree, mainLocator, mainAuthority, logFrame, mainFrameLine, mainSuite, mainExecution, verifyMergedPromotion,
  CONTROLLER_PREFIX, CONTROLLER_KIND, controllerTree, verifyControllerMaintenance, controllerLogFrame, controllerFrameLine };
