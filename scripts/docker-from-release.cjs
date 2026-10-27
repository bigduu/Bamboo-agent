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
const IMAGE_STEPS = [SELECT, "Copy authenticated binary and Host originals", BUILD, SMOKE, PUSH, UPLOAD];
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
  const controller = caller(), env = process.env, event = JSON.parse(fs.readFileSync(env.GITHUB_EVENT_PATH, "utf8"));
  const runId = eventRun(env.GITHUB_EVENT_NAME, event, env.DOCKER_RELEASE_RUN);
  // The inherited trusted adapter has no policy/token/fixture override.
  const reads = require("./main-admission.cjs").createPublicReads();
  const input = await locate(reads, runId), facts = await proof.verifyPublishedSource(input, reads);
  assert.equal(facts.publicationAuthorized, false); core.assertVersion(facts.release.version);
  return { controller, reads, input, facts };
}
async function refreshPublication(held) {
  // Re-run the real zero-argument caller/public verifier after long carrier
  // reads. JSON, callbacks and the consumer run alone grant no write authority.
  const fresh = await authenticated(), identity = value => ({ consumerController: value.controller, input: value.input,
    source: value.facts.source, release: value.facts.release, controller: value.facts.controller, completionSha256: value.facts.completionSha256 });
  assert.deepEqual(identity(fresh), identity(held), "Completed publication changed before registry write");
}
function target(platform) {
  assert.ok(PLATFORMS.includes(platform), "Unknown Docker architecture");
  assert.equal(platform, "linux/amd64", "Docker linux/arm64 needs authenticated original producer #2353; no compile or amd64 fallback");
  return "x86_64-unknown-linux-gnu";
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
  const platform = process.env.DOCKER_PLATFORM, triple = target(platform), state = await authenticated();
  const execution = await consumerExecution(state, "image"); passed(step(execution.current, SELECT));
  assert.ok(!imageHistory(execution.jobs, execution.run, platform, execution.run.run_attempt).physical, "Stage cannot replace an original image");
  await noPriorCarrier(execution, platform);
  const facts = state.facts, binaryRef = facts.native.assets.find(value => value.name === sidecar.artifactName(triple)); assert.ok(binaryRef);
  const hostRef = facts.native.assets.find(value => value.name === sidecar.HOST_ASSET), manifestRef = facts.native.assets.find(value => value.name === sidecar.MANIFEST);
  assert.ok(hostRef && manifestRef); const manifest = JSON.parse(await original(state.reads, manifestRef));
  assert.equal(manifest.sourceRevision, facts.source.revision); assert.equal(manifest.version, facts.release.version);
  const directory = path.join(path.resolve(process.env.RUNNER_TEMP), "bamboo-docker-context"); fs.mkdirSync(directory);
  const binary = await original(state.reads, binaryRef), file = path.join(directory, "bamboo"); fs.writeFileSync(file, binary, { flag: "wx", mode: 0o755 });
  const abi = elfReport(command("readelf", ["--wide", "--file-header", "--dynamic", "--version-info", file]), platform);
  const host = hostFiles(await original(state.reads, hostRef), manifest.browserHost.files); fs.mkdirSync(path.join(directory, "browser-runtime"));
  for (const [name, bytes] of Object.entries(host)) fs.writeFileSync(path.join(directory, name), bytes, { flag: "wx", mode: 0o644 });
  fs.copyFileSync(path.join(ROOT, "docker/Dockerfile"), path.join(directory, "Dockerfile"), fs.constants.COPYFILE_EXCL);
  const descriptor = { ...selected(facts), platform, consumer: state.controller, binary: binaryRef, host: hostRef, abi, runtime: RUNTIME };
  fs.writeFileSync(path.join(directory, "originals.json"), JSON.stringify(descriptor, null, 2) + "\n", { flag: "wx" });
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
  if (exactController) {
    assert.equal(run.head_sha, exactController, "Unproven prior controller metadata role; stop for Ops");
    assert.equal(suite.workflowRun.file.repositoryFileUrl, `https://github.com/${sidecar.REPOSITORY}/blob/${exactController}/${WORKFLOW}`);
  }
}
async function consumerExecution(state, role) {
  const env = process.env; assert.equal(env.GITHUB_JOB, role, "Actual job key must match CLI phase");
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
  const name = role === "image" ? "Repackage originals (" + process.env.DOCKER_PLATFORM + ")" : role === "source" ? SOURCE_JOB : "Publish complete multi-arch manifest";
  const current = jobs.filter(job => job.name === name && job.run_attempt === run.run_attempt); assert.equal(current.length, 1); assert.equal(current[0].status, "in_progress");
  return { get, paged, run, jobs, current: current[0] };
}
function sourceLine(state, consumerRun) {
  return SOURCE_PREFIX + Buffer.from(core.canonicalJson({ ...selected(state.facts), consumerRun, controller: state.controller })).toString("base64");
}
function sourceLocator(log, run, physical) {
  assert.ok(typeof log === "string" && Buffer.byteLength(log) <= 64 * 1024 * 1024);
  const lines = log.split(/\r?\n/).filter(line => /^\d{4}-\d\d-\d\dT\S+Z /.test(line) && line.slice(line.indexOf(" ") + 1).startsWith(SOURCE_PREFIX)); assert.equal(lines.length, 1, "Missing/ambiguous original consumer source locator");
  passed(step(physical, SOURCE_STEP)); const time = Date.parse(lines[0].split(" ")[0]), selectedStep = step(physical, SOURCE_STEP);
  const second = value => Math.floor(Date.parse(value) / 1000);
  assert.ok(Number.isFinite(time) && second(selectedStep.started_at) <= Math.floor(time / 1000) && Math.floor(time / 1000) <= second(selectedStep.completed_at));
  const encoded = lines[0].slice(lines[0].indexOf(SOURCE_PREFIX) + SOURCE_PREFIX.length); assert.ok(encoded.length <= 4096 && /^[A-Za-z0-9+/]+={0,2}$/.test(encoded));
  const bytes = Buffer.from(encoded, "base64"), value = JSON.parse(bytes.toString("utf8")); assert.equal(bytes.toString("base64"), encoded); assert.equal(core.canonicalJson(value), bytes.toString("utf8"));
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
function physicalUpload(log, job, artifact, runId) {
  assert.ok(typeof log === "string" && Buffer.byteLength(log) <= 64 * 1024 * 1024);
  const upload = step(job, UPLOAD); passed(upload);
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
module.exports = { RUNTIME, IMAGE, PLATFORMS, SELECT, BUILD, SMOKE, PUSH, UPLOAD, REUSE, SOURCE_JOB, SOURCE_STEP, SOURCE_PREFIX,
  safeEnv, eventRun, locate, target, verifyBytes, original, hostFiles, elfReport, imageReference, candidateImage, digestFile, smokeCommands, digestRecord,
  manifestReport, latestIdentity, publicationTags, verifyLatest, consumerSuite, consumerExecution, freshConsumer, sourceLine, sourceLocator, crossRunGuard, imageHistory, decodeCarrier, readCarrier, noPriorCarrier, authenticated };
if (require.main === module) main().catch(error => { console.error("Published Docker consumer: " + error.message); process.exitCode = 1; });
