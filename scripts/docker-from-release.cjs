#!/usr/bin/env node
"use strict";
// Distribution consumes authenticated originals. This module has no source
// writer, allocator, compiler, frontend builder or original-output lifecycle.
const assert = require("node:assert/strict"), fs = require("node:fs"), path = require("node:path");
const { spawnSync } = require("node:child_process");
const core = require("./crate-release.cjs"), sidecar = require("./sidecar-release.cjs");
const proof = require("./published-source-admission.cjs"), native = require("./native-originals.cjs");
const ROOT = path.resolve(__dirname, ".."), WORKFLOW = ".github/workflows/docker-publish.yml";
// Official registry index/config metadata read 2026-10-27; both architectures
// resolve to Node24.22.1. No image layer was downloaded to choose this pin.
const RUNTIME = "node:24-bookworm-slim@sha256:55fb5f5a527ea2c5c4a95f963e48ef90aa38c7b346054fdc8ee9957a2b55d205";
const PLATFORMS = Object.freeze(["linux/amd64", "linux/arm64"]);
const IMAGE = "ghcr.io/" + sidecar.REPOSITORY.split("/")[0].toLowerCase() + "/bamboo";
const SHA = /^[0-9a-f]{40}$/, DIGEST = /^sha256:[0-9a-f]{64}$/;
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
  const files = JSON.parse(command("python3", ["-I", "-c", HOST_READER], { input: bytes }));
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
function readDigests(facts) {
  const directory = path.join(process.env.RUNNER_TEMP, "docker-digests"), records = [];
  for (const name of fs.readdirSync(directory)) {
    assert.match(name, /^docker-digest-[1-9][0-9]*-[1-9][0-9]*-(?:amd64|arm64)\.json$/);
    const file = path.join(directory, name), info = fs.lstatSync(file); assert.ok(info.isFile() && !info.isSymbolicLink() && info.size > 0 && info.size <= 4096);
    const record = digestRecord(JSON.parse(fs.readFileSync(file)), facts);
    assert.match(name, new RegExp("^docker-digest-" + positive(process.env.GITHUB_RUN_ID) + "-[1-9][0-9]*-" + record.platform.slice(6) + "\\.json$")); records.push(record);
  }
  assert.equal(records.length, 2, "Both original architecture digests are required; ambiguous or missing evidence stops");
  assert.deepEqual(records.map(value => value.platform).sort(), [...PLATFORMS].sort()); return records;
}
function manifestReport(value, records) {
  assert.match(value.digest, DIGEST); assert.equal(value.manifests.length, 2);
  const actual = value.manifests.map(item => ({ platform: item.platform.os + "/" + item.platform.architecture, digest: item.digest }));
  assert.deepEqual(actual.sort((a,b) => a.platform.localeCompare(b.platform)), records.map(({ platform, digest }) => ({ platform, digest })).sort((a,b) => a.platform.localeCompare(b.platform)));
  return value.digest;
}
async function main() {
  const [phase] = process.argv.slice(2); assert.equal(process.argv.length, 3);
  if (phase === "stage") return stage(); if (phase === "smoke") return smoke();
  const state = await authenticated(), facts = state.facts;
  if (phase === "source" || phase === "guard") { output("release_run", state.input.releaseRun); output("source", facts.source.revision); output("version", facts.release.version); return; }
  if (phase === "push") {
    const platform = process.env.DOCKER_PLATFORM; target(platform);
    const image = candidateImage(process.env.GITHUB_RUN_ID, process.env.GITHUB_RUN_ATTEMPT, platform); assert.equal(process.env.DOCKER_IMAGE, image);
    // Fresh real completed-source proof above is immediately before the write.
    command("docker", ["push", image], { timeout: 15 * 60000 });
    const references = JSON.parse(command("docker", ["image", "inspect", "--format", "{{json .RepoDigests}}", image]));
    const matching = references.filter(value => value.startsWith(IMAGE + "@")); assert.equal(matching.length, 1);
    const record = digestRecord({ ...selected(facts), platform, digest: matching[0].slice(IMAGE.length + 1) }, facts);
    fs.writeFileSync(path.join(process.env.RUNNER_TEMP, digestFile(process.env.GITHUB_RUN_ID, process.env.GITHUB_RUN_ATTEMPT, platform)), JSON.stringify(record) + "\n", { flag: "wx" }); return;
  }
  assert.equal(phase, "publish"); const records = readDigests(facts), versionTag = IMAGE + ":v" + facts.release.version;
  command("docker", ["buildx", "imagetools", "create", "--tag", versionTag, "--tag", IMAGE + ":latest", ...records.map(value => IMAGE + "@" + value.digest)], { timeout: 5 * 60000 });
  // Use Buildx's descriptor digest, not a hash of CLI-formatted JSON/newlines.
  const inspect = name => JSON.parse(command("docker", ["buildx", "imagetools", "inspect", "--format", "{{json .Manifest}}", name]));
  const digest = manifestReport(inspect(versionTag), records); assert.equal(manifestReport(inspect(IMAGE + ":latest"), records), digest);
  output("image", IMAGE + "@" + digest);
}
module.exports = { RUNTIME, IMAGE, PLATFORMS, safeEnv, eventRun, locate, target, verifyBytes, original, hostFiles, elfReport, imageReference, candidateImage, digestFile, smokeCommands, digestRecord, readDigests, manifestReport, authenticated };
if (require.main === module) main().catch(error => { console.error("Published Docker consumer: " + error.message); process.exitCode = 1; });
