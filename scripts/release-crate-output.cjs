"use strict";

// V2 crate-output lifecycle only. The caller owns authenticated reservation
// load/save, source admission and the existing guarded Release writes. Neither
// a ZIP nor a readVerified* result grants publication/Main authority.
// Ports: load/save use the existing authenticated reservation's single writer;
// read/readAsset are fixed-repository GETs; uploadAsset is the existing guarded,
// non-replacing Release upload; prepare uses prepareFromSandbox. Public readers
// still authenticate the enclosing finalizer/producer and complete source proof.
// Registry upload is an explicit port. Its external source-authority guard is
// mandatory immediately before every PUT, including after retry waits/resume.
const assert = require("node:assert/strict");
const crypto = require("node:crypto");
const fs = require("node:fs");
const { spawnSync } = require("node:child_process");
const sandbox = require("./release-cargo-sandbox.cjs");
const uploader = require("./release-cargo-upload.cjs");
const REPOSITORY = "bigduu/Bamboo-agent";
const REPOSITORY_ID = 1164779771;
const WORKFLOW = ".github/workflows/publish-crate.yml";
const JOB = "Publish and verify the crate closure";
const KIND = "bamboo-verified-crate/v1";
const MAX_ARCHIVE = 64 * 1024 ** 2, MAX_PROOF = 64 * 1024, MAX_ZIP = MAX_ARCHIVE + MAX_PROOF + 1024;
const SHA = /^[0-9a-f]{40}$/, DIGEST = /^[0-9a-f]{64}$/;
const hash = bytes => crypto.createHash("sha256").update(bytes).digest("hex");
const clone = value => JSON.parse(JSON.stringify(value));
const keys = (value, expected) => assert.deepEqual(Object.keys(value || {}).sort(), [...expected].sort(), "Unexpected crate-output fields");
const positive = value => assert.ok(Number.isSafeInteger(value) && value > 0, "Positive numeric identity required");
const command = name => ["package", "--locked", "--allow-dirty", "-p", name];
function inputs(value) {
  keys(value, ["repository", "releaseId", "sourceRevision", "sourceTree", "version", "name", "pathInVcs", "frontendBytes"]);
  assert.equal(value.repository, REPOSITORY); positive(value.releaseId);
  for (const key of ["sourceRevision", "sourceTree"]) assert.match(value[key], SHA);
  assert.match(value.version, /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-[0-9A-Za-z]+(?:[.-][0-9A-Za-z]+)*)?$/);
  assert.notEqual(value.version, "0.0.0"); assert.ok(value.version.length <= 96);
  for (const part of value.version.split("-")[0].split(".")) assert.ok(part.length <= 20 && BigInt(part) <= 18446744073709551615n);
  if (value.version.includes("-")) for (const part of value.version.slice(value.version.indexOf("-") + 1).split(".")) assert.ok(!/^0[0-9]+$/.test(part), "Invalid numeric prerelease");
  assert.match(value.name, /^[A-Za-z0-9_-]{1,64}$/);
  assert.ok(typeof value.pathInVcs === "string" && (value.pathInVcs === "" || /^[A-Za-z0-9_.-]+(?:\/[A-Za-z0-9_.-]+)*$/.test(value.pathInVcs)));
  assert.ok(!value.pathInVcs.split("/").some(part => [".", ".."].includes(part)));
  keys(value.frontendBytes, ["manifestSha256", "archiveSha256"]);
  for (const digest of Object.values(value.frontendBytes)) assert.match(digest, DIGEST);
  return value;
}
function origin(value) {
  keys(value, ["controllerRevision", "runId", "attempt", "jobId", "jobKey", "jobName", "workflowPath"]);
  assert.match(value.controllerRevision, SHA);
  for (const key of ["runId", "attempt", "jobId"]) positive(value[key]);
  assert.equal(value.jobKey, "publish-crates"); assert.equal(value.jobName, JOB); assert.equal(value.workflowPath, WORKFLOW);
  return value;
}
function verification(value, name) {
  keys(value, ["preparationCommand", "command", "image", "exitCode"]);
  assert.deepEqual(value.preparationCommand, ["package", "--locked", "--allow-dirty", "--no-verify", "-p", name]);
  assert.deepEqual(value.command, command(name)); assert.equal(value.image, sandbox.IMAGE); assert.equal(value.exitCode, 0);
}
function archiveIdentity(value, expected) {
  keys(value, ["name", "size", "sha256"]);
  assert.equal(value.name, `${expected.name}-${expected.version}.crate`);
  positive(value.size); assert.ok(value.size <= MAX_ARCHIVE); assert.match(value.sha256, DIGEST);
}
function proof(value, expected) {
  keys(value, ["schemaVersion", "kind", "inputs", "origin", "archive", "verification"]);
  assert.equal(value.schemaVersion, 1); assert.equal(value.kind, KIND); inputs(value.inputs);
  assert.deepEqual(value.inputs, inputs(expected)); origin(value.origin); archiveIdentity(value.archive, expected);
  verification(value.verification, expected.name); return value;
}
function record(value, expected) {
  assert.ok(["preparing", "verified", "published"].includes(value?.phase), "Unsupported crate-output phase");
  const fields = ["schemaVersion", "kind", "inputs", "origin", "phase"];
  if (value.sealed !== undefined) fields.push("sealed");
  if (value.phase !== "preparing") fields.push("asset");
  keys(value, fields); assert.equal(value.schemaVersion, 1); assert.equal(value.kind, KIND);
  assert.deepEqual(inputs(value.inputs), inputs(expected)); origin(value.origin);
  if (value.sealed !== undefined) {
    keys(value.sealed, ["size", "sha256", "proof"]); positive(value.sealed.size); assert.ok(value.sealed.size <= MAX_ZIP);
    assert.match(value.sealed.sha256, DIGEST); proof(value.sealed.proof, expected);
    assert.deepEqual(value.sealed.proof.origin, value.origin);
  }
  if (value.phase !== "preparing") {
    assert.ok(value.sealed, "Verified output requires its pre-upload sealed checkpoint");
    keys(value.asset, ["id", "name", "size", "digest"]); positive(value.asset.id);
    assert.equal(value.asset.name, assetName(expected)); assert.equal(value.asset.size, value.sealed.size);
    assert.equal(value.asset.digest, `sha256:${value.sealed.sha256}`);
  }
  return value;
}
const assetName = expected => `bamboo-verified-crate-${expected.name}-${expected.version}.zip`;
const env = () => Object.fromEntries(["PATH", "HOME"].filter(key => process.env[key] !== undefined).map(key => [key, process.env[key]]));
function python(code, bytes, args = [], maximum = MAX_ZIP) {
  const result = spawnSync("python3", ["-I", "-c", code, ...args], { input: bytes, env: env(), maxBuffer: maximum, timeout: 30_000 });
  assert.ok(!result.error && result.status === 0, "Invalid or unsupported crate-output bytes (Python 3.11+ required)");
  return result.stdout;
}
const ZIP = String.raw`
import io,json,stat,struct,sys,zipfile
mode=sys.argv[1]; raw=sys.stdin.buffer.read(64*1024*1024+65536+1025)
assert len(raw)<=64*1024*1024+65536+1024
if mode=='pack':
    size=struct.unpack('<I',raw[:4])[0]; assert 0<size<=65536
    proof,archive=raw[4:4+size],raw[4+size:]; assert 0<len(archive)<=64*1024*1024
    json.loads(proof); output=io.BytesIO()
    with zipfile.ZipFile(output,'w',compression=zipfile.ZIP_STORED) as z:
        for name,data in [('verification.json',proof),('package.crate',archive)]:
            item=zipfile.ZipInfo(name); item.create_system=3; item.external_attr=(stat.S_IFREG|0o644)<<16
            z.writestr(item,data)
    sys.stdout.buffer.write(output.getvalue())
else:
    assert mode=='unpack'
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        entries=z.infolist(); assert sorted(i.filename for i in entries)==['package.crate','verification.json']
        for item in entries:
            assert item.create_system==3 and stat.S_ISREG(item.external_attr>>16)
            assert not item.flag_bits&1 and item.compress_type==zipfile.ZIP_STORED
            assert 0<item.file_size<=(65536 if item.filename=='verification.json' else 64*1024*1024)
        proof=z.read('verification.json'); archive=z.read('package.crate')
        sys.stdout.buffer.write(struct.pack('<I',len(proof))+proof+archive)
`;
function seal(bytes, value) {
  proof(value, value.inputs); assert.ok(Buffer.isBuffer(bytes) && bytes.length === value.archive.size && hash(bytes) === value.archive.sha256);
  const json = Buffer.from(JSON.stringify(value)); assert.ok(json.length <= MAX_PROOF);
  const size = Buffer.alloc(4); size.writeUInt32LE(json.length);
  return python(ZIP, Buffer.concat([size, json, bytes]), ["pack"]);
}
function unseal(zip, expected) {
  assert.ok(Buffer.isBuffer(zip) && zip.length > 0 && zip.length <= MAX_ZIP);
  const raw = python(ZIP, zip, ["unpack"]), size = raw.readUInt32LE(0);
  assert.ok(size > 0 && size <= MAX_PROOF && raw.length > 4 + size);
  const json = raw.subarray(4, 4 + size), value = proof(JSON.parse(json), expected), bytes = Buffer.from(raw.subarray(4 + size));
  assert.equal(json.toString(), JSON.stringify(value), "Noncanonical crate proof");
  assert.equal(bytes.length, value.archive.size); assert.equal(hash(bytes), value.archive.sha256);
  return { proof: value, bytes };
}
function inspectArchive(bytes, expected) {
  inputs(expected); assert.ok(Buffer.isBuffer(bytes) && bytes.length > 0 && bytes.length <= MAX_ARCHIVE);
  const metadata = uploader.describeArchive(bytes, expected.name, expected.version);
  const observed = JSON.parse(python(String.raw`
import hashlib,io,json,sys,tarfile
name,version=sys.argv[1:]; data=sys.stdin.buffer.read(64*1024*1024+1); assert len(data)<=64*1024*1024
with tarfile.open(fileobj=io.BytesIO(data),mode='r:gz') as archive:
    def read(relative):
        member=archive.getmember(name+'-'+version+'/'+relative)
        assert member.isfile() and member.size<=64*1024*1024
        return archive.extractfile(member).read()
    result={'vcs':json.loads(read('.cargo_vcs_info.json'))}
    if name=='bamboo-server': result['frontend']={key:hashlib.sha256(read(file)).hexdigest() for key,file in [('manifestSha256','frontend_package/frontend-manifest.json'),('archiveSha256','frontend_package/lotus-frontend.zip')]}
    print(json.dumps(result))
`, bytes, [expected.name, expected.version], 8192));
  assert.equal(observed.vcs.git?.sha1, expected.sourceRevision, "Crate source differs");
  assert.equal(observed.vcs.git?.dirty, true, "Final-version archive must retain its actual stamped dirty state");
  assert.equal(observed.vcs.path_in_vcs, expected.pathInVcs, "Crate layout differs");
  if (expected.name === "bamboo-server") assert.deepEqual(observed.frontend, expected.frontendBytes, "Frontend archive differs");
  return metadata;
}
function prepareFromSandbox(state, expected) {
  inputs(expected); assert.equal(state.revision, expected.sourceRevision);
  const args = command(expected.name), preparation = ["package", "--locked", "--allow-dirty", "--no-verify", "-p", expected.name];
  const file = sandbox.packageFile(state, args); assert.equal(file, `${expected.name}-${expected.version}.crate`);
  // Preserve the existing controller-held pre-verification checksum boundary.
  // Two cheap package operations, one sandbox verification compilation. The
  // second worker cannot mount or replace this first controller-owned Buffer.
  assert.equal(sandbox.run(state, preparation), 0, "Crate preparation did not complete");
  const original = fs.readFileSync(sandbox.regular(state.target, `package/${file}`));
  inspectArchive(original, expected); const checksum = hash(original);
  assert.equal(sandbox.run(state, args), 0, "Crate package/verification did not complete");
  const verified = fs.readFileSync(sandbox.regular(state.target, `package/${file}`));
  assert.equal(hash(verified), checksum, "Verified package archive changed from the controller's original bytes");
  return { bytes: original, verification: { preparationCommand: preparation, command: args, image: sandbox.IMAGE, exitCode: 0 } };
}
async function jobs(runId, read) {
  const values = [], ids = new Set(); let total;
  for (let page = 1; page <= 20; page++) {
    const response = await read(`actions/runs/${runId}/jobs?filter=all&per_page=100&page=${page}`);
    assert.ok(Array.isArray(response?.jobs) && response.jobs.length <= 100);
    assert.ok(Number.isSafeInteger(response.total_count) && response.total_count >= 0);
    total ??= response.total_count; assert.equal(response.total_count, total);
    for (const item of response.jobs) { positive(item.id); assert.ok(!ids.has(item.id)); ids.add(item.id); values.push(item); }
    assert.ok(values.length <= total);
    if (values.length === total) return values;
    assert.equal(response.jobs.length, 100, "Truncated execution evidence");
  }
  assert.fail("Execution evidence exceeds limit");
}
async function readOrigin(value, read) {
  origin(value);
  const run = await read(`actions/runs/${value.runId}`);
  assert.equal(run?.id, value.runId); assert.equal(run.repository?.id, REPOSITORY_ID); assert.equal(run.repository?.full_name, REPOSITORY);
  assert.equal(run.head_repository?.id, REPOSITORY_ID); assert.equal(run.head_repository?.full_name, REPOSITORY);
  assert.equal(run.event, "workflow_dispatch"); assert.equal(run.path, WORKFLOW); assert.equal(run.head_branch, "main");
  assert.equal(run.head_sha, value.controllerRevision); positive(run.run_attempt); assert.ok(run.run_attempt >= value.attempt);
  const rows = await jobs(value.runId, read), selected = rows.filter(job => job.id === value.jobId);
  assert.equal(selected.length, 1, "Original numeric job disappeared"); const job = selected[0];
  assert.equal(job.run_id, value.runId); assert.equal(job.run_attempt, value.attempt); assert.equal(job.name, value.jobName);
  assert.equal(job.head_sha, value.controllerRevision); assert.equal(job.head_branch, "main");
  assert.ok(["in_progress", "completed"].includes(job.status));
  assert.ok(job.status === "in_progress" ? job.conclusion === null : ["success", "failure", "cancelled", "timed_out"].includes(job.conclusion));
  assert.ok(Number.isFinite(Date.parse(job.started_at)));
  if (job.status === "completed") assert.ok(Date.parse(job.completed_at) >= Date.parse(job.started_at));
  const direct = await read(`actions/jobs/${value.jobId}`);
  for (const key of ["id", "run_id", "run_attempt", "name", "head_sha", "head_branch", "status", "conclusion", "started_at", "completed_at"]) assert.deepEqual(direct?.[key], job[key], "Original job changed");
  return { run, job };
}
async function executionOrigin(environment, read) {
  assert.equal(environment.GITHUB_REPOSITORY, REPOSITORY); assert.equal(environment.GITHUB_EVENT_NAME, "workflow_dispatch");
  assert.equal(environment.GITHUB_REF, "refs/heads/main"); assert.equal(environment.GITHUB_SHA, environment.GITHUB_WORKFLOW_SHA);
  assert.equal(environment.GITHUB_WORKFLOW_REF, `${REPOSITORY}/${WORKFLOW}@refs/heads/main`); assert.equal(environment.GITHUB_JOB, "publish-crates");
  for (const key of ["GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT"]) assert.match(environment[key], /^[1-9][0-9]*$/);
  const runId = Number(environment.GITHUB_RUN_ID), attempt = Number(environment.GITHUB_RUN_ATTEMPT); positive(runId); positive(attempt);
  const rows = await jobs(runId, read), selected = rows.filter(job => job.run_attempt === attempt && job.name === JOB && job.status === "in_progress" && job.conclusion === null);
  assert.equal(selected.length, 1, "Exactly one real current crate job required");
  const value = { controllerRevision: environment.GITHUB_WORKFLOW_SHA, runId, attempt, jobId: selected[0].id,
    jobKey: environment.GITHUB_JOB, jobName: JOB, workflowPath: WORKFLOW };
  await readOrigin(value, read); return value;
}
async function assets(expected, read) {
  const release = await read(`releases/${expected.releaseId}`);
  assert.equal(release?.id, expected.releaseId); assert.equal(release.tag_name, `v${expected.version}`);
  assert.equal(release.target_commitish, expected.sourceRevision);
  const values = [], ids = new Set();
  for (let page = 1; page <= 20; page++) {
    const batch = await read(`releases/${expected.releaseId}/assets?per_page=100&page=${page}`);
    assert.ok(Array.isArray(batch) && batch.length <= 100);
    for (const item of batch) { positive(item.id); assert.ok(!ids.has(item.id)); ids.add(item.id); values.push(item); }
    if (batch.length < 100) return values;
  }
  assert.fail("Release asset evidence exceeds limit");
}
function assetFields(asset) { return Object.fromEntries(["id", "name", "size", "digest"].map(key => [key, asset[key]])); }
async function readSealed(state, expected, ports, selected) {
  expected = clone(inputs(expected)); state = clone(record(state, expected));
  assert.ok(state.sealed, "Preparing output has no authenticated sealed hash");
  const found = (await assets(expected, ports.read)).filter(asset => asset.name === assetName(expected)); assert.equal(found.length, 1, "Original crate ZIP missing or ambiguous");
  const asset = clone(found[0]);
  if (selected) assert.deepEqual(assetFields(asset), selected, "Original crate ZIP was replaced");
  positive(asset.id); assert.equal(asset.state, "uploaded"); assert.equal(asset.size, state.sealed.size); assert.equal(asset.digest, `sha256:${state.sealed.sha256}`);
  const direct = await ports.read(`releases/assets/${asset.id}`); assert.deepEqual(assetFields(direct), assetFields(asset)); assert.equal(direct.state, "uploaded"); assert.equal(direct.created_at, asset.created_at);
  const zip = Buffer.from(await ports.readAsset(asset.id)); assert.equal(zip.length, state.sealed.size); assert.equal(hash(zip), state.sealed.sha256);
  const output = unseal(zip, expected); assert.deepEqual(output.proof, state.sealed.proof); inspectArchive(output.bytes, expected);
  const observed = await readOrigin(state.origin, ports.read), created = Date.parse(asset.created_at);
  assert.ok(Number.isFinite(created) && created >= Date.parse(observed.job.started_at), "ZIP predates its producer");
  if (observed.job.status === "completed") assert.ok(created <= Date.parse(observed.job.completed_at), "ZIP outside original job");
  const fresh = await ports.read(`releases/assets/${asset.id}`);
  assert.deepEqual(assetFields(fresh), assetFields(asset)); assert.equal(fresh.state, "uploaded"); assert.equal(fresh.created_at, asset.created_at);
  return { ...output, asset: assetFields(asset), observedJob: { id: observed.job.id, attempt: observed.job.run_attempt,
    status: observed.job.status, conclusion: observed.job.conclusion } };
}
async function readVerifiedCrate(state, expected, ports) {
  expected = clone(inputs(expected)); state = clone(record(state, expected));
  record(state, expected); assert.ok(["verified", "published"].includes(state.phase), "No verified original crate checkpoint");
  return { ...await readSealed(state, expected, ports, state.asset), publicationAuthorized: false };
}
async function registryProof(expected, bytes, ports) {
  const result = await ports.registry(expected.name, expected.version);
  if (result === null) return false;
  assert.equal(result?.checksum, hash(bytes), "Registry checksum differs from verified archive");
  const published = Buffer.from(await ports.registryArchive(expected.name, expected.version));
  assert.ok(published.length <= MAX_ARCHIVE); assert.deepEqual(published, bytes, "Registry archive differs from verified original");
  inspectArchive(published, expected); return true;
}
async function readVerifiedClosure(expected, states, ports) {
  expected = clone(expected); states = clone(states);
  assert.ok(Array.isArray(expected) && expected.length > 0 && expected.length <= 128); assert.equal(states.length, expected.length);
  assert.equal(new Set(expected.map(value => inputs(value).name)).size, expected.length);
  const result = [];
  for (let index = 0; index < expected.length; index++) {
    const state = record(states[index], expected[index]); assert.equal(state.phase, "published");
    const value = await readVerifiedCrate(state, expected[index], ports); assert.equal(value.observedJob.status, "completed");
    assert.ok(await registryProof(expected[index], value.bytes, ports), "Published crate missing from registry");
    result.push({ name: expected[index].name, archive: value.proof.archive, origin: value.proof.origin,
      verification: value.proof.verification, asset: value.asset, originalJob: value.observedJob });
  }
  return { kind: "bamboo-verified-crate-closure/v1", publicationAuthorized: false, crates: result };
}
async function publishCrate(expected, ports) {
  expected = clone(inputs(expected));
  assert.equal(typeof ports.guard, "function", "Fresh source guard is required for every registry PUT");
  assert.equal(typeof ports.upload, "function", "Explicit archive upload port is required");
  let state = await ports.load(); if (state !== null) state = clone(record(state, expected));
  const save = async next => {
    next = clone(record(next, expected));
    try { await ports.save(clone(next), state === null ? null : clone(state)); }
    catch { /* The authenticated read-back also resolves a lost PATCH response. */ }
    const fresh = await ports.load();
    assert.deepEqual(fresh, next, "Authenticated crate checkpoint save is unconfirmed");
    state = clone(record(fresh, expected));
  };
  if (state === null) {
    assert.equal(await ports.registry(expected.name, expected.version), null, "Registry version has no verified original checkpoint");
    assert.equal((await assets(expected, ports.read)).filter(asset => asset.name === assetName(expected)).length, 0, "Unclaimed crate output exists");
    const current = await executionOrigin(ports.environment || process.env, ports.read);
    await save({ schemaVersion: 1, kind: KIND, inputs: clone(expected), origin: current, phase: "preparing" });
    const prepared = await ports.prepare(clone(expected));
    verification(prepared.verification, expected.name); const bytes = Buffer.from(prepared.bytes); inspectArchive(bytes, expected);
    const value = { schemaVersion: 1, kind: KIND, inputs: clone(expected), origin: current,
      archive: { name: `${expected.name}-${expected.version}.crate`, size: bytes.length, sha256: hash(bytes) }, verification: prepared.verification };
    const zip = seal(bytes, value);
    // Persist the authenticated exact ZIP hash before exposing it in Release
    // storage. A merely self-described ZIP can never recover a pending intent.
    await save({ ...state, sealed: { size: zip.length, sha256: hash(zip), proof: value } });
    try { await ports.uploadAsset(assetName(expected), Buffer.from(zip)); }
    catch { /* An acknowledged immutable read-back, not the write response, decides. */ }
  }
  if (state.phase === "preparing") {
    assert.ok(state.sealed, "Prior verification outcome is unknown; never repackage pending work");
    const value = await readSealed(state, expected, ports); await save({ ...state, phase: "verified", asset: value.asset });
  }
  const output = await readVerifiedCrate(state, expected, ports), metadata = inspectArchive(output.bytes, expected);
  const published = async () => { if (!await registryProof(expected, output.bytes, ports)) return false;
    if (state.phase !== "published") await save({ ...state, phase: "published" }); return true; };
  if (await published()) return clone(state);
  assert.notEqual(state.phase, "published", "Previously published registry original disappeared");
  for (let attempt = 0; attempt < 25; attempt++) {
    // Each call owns a fresh Buffer. Every attempt still hashes to the same
    // authenticated archive; neither a callback nor an await can replace it.
    await ports.guard();
    const result = await ports.upload(Buffer.from(output.bytes), clone(metadata));
    if (result?.status === 0 || /already exists on crates.io index/.test(result?.output || "")) {
      for (let poll = 0; poll < 30; poll++) { if (await published()) return clone(state); if (poll < 29) await ports.wait(10); }
      assert.fail("Registry upload is not confirmed; retain verified original for explicit resume");
    }
    if (!/429 Too Many Requests/.test(result?.output || "") || attempt === 24) {
      if (await published()) return clone(state);
      assert.fail("Registry upload is unconfirmed; resume only the same verified bytes");
    }
    const date = result.output.match(/try again after ([A-Za-z]{3}, \d{2} [A-Za-z]{3} \d{4} \d{2}:\d{2}:\d{2} GMT)/)?.[1];
    const delay = date && Number.isFinite(Date.parse(date)) ? Math.ceil((Date.parse(date) - Date.now()) / 1000) + 10 : 120;
    await ports.wait(Math.max(15, Math.min(1200, delay)));
    if (await published()) return clone(state);
  }
  assert.fail("Registry retry limit exhausted");
}
module.exports = { REPOSITORY, WORKFLOW, JOB, KIND, MAX_ARCHIVE, MAX_ZIP, inputs, record, assetName, inspectArchive,
  seal, unseal, prepareFromSandbox, executionOrigin, readVerifiedCrate, readVerifiedClosure, publishCrate };
