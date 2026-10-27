"use strict";
// Data/transport/static-routing fixtures only: no Docker, Cargo or browser run.
const assert = require("node:assert/strict"), test = require("node:test");
const fs = require("node:fs"), os = require("node:os"), path = require("node:path");
const { spawnSync } = require("node:child_process");
const consumer = require("./docker-from-release.cjs"), core = require("./crate-release.cjs");
const ROOT = path.resolve(__dirname, ".."), text = file => fs.readFileSync(path.join(ROOT, file), "utf8");
const facts = { source: { revision: "a".repeat(40) }, release: { version: "2026.10.27", id: 30 }, controller: { runId: 10, revision: "b".repeat(40) } };
const digest = digit => "sha256:" + digit.repeat(64);
function record(platform = "linux/amd64") { return { source: facts.source.revision, version: facts.release.version, releaseRun: 10, publisher: "b".repeat(40), releaseId: 30, platform, digest: digest(platform.endsWith("amd64") ? "1" : "2") }; }
function host() {
  return { "browser-runtime/host.cjs": Buffer.from("// tiny Host archive data, never executed\n"),
    "browser-runtime/package.json": fs.readFileSync(path.join(ROOT, "browser-runtime/package.json")),
    "browser-runtime/package-lock.json": fs.readFileSync(path.join(ROOT, "browser-runtime/package-lock.json")) };
}
const entries = files => Object.entries(files).map(([name, bytes]) => ({ name, bytes: bytes.toString("base64") }));
const manifest = files => Object.entries(files).map(([name, bytes]) => ({ path: name, sizeBytes: bytes.length, sha256: core.sha256(bytes) }));
function tar(rows) {
  const script = `import base64,io,json,sys,tarfile
out=io.BytesIO()
with tarfile.open(fileobj=out,mode='w:gz',format=tarfile.USTAR_FORMAT) as archive:
 for row in json.load(sys.stdin):
  info=tarfile.TarInfo(row['name']); raw=base64.b64decode(row.get('bytes','')); info.size=len(raw)
  if row.get('link'): info.type=tarfile.SYMTYPE; info.linkname=row['link']; info.size=0
  archive.addfile(info,io.BytesIO(raw))
sys.stdout.buffer.write(out.getvalue())`;
  const result = spawnSync("python3", ["-I", "-c", script], { input: JSON.stringify(rows), timeout: 10000,
    env: Object.fromEntries(["PATH", "HOME", "TMPDIR"].filter(key => process.env[key] !== undefined).map(key => [key, process.env[key]])) });
  assert.equal(result.status, 0); return result.stdout;
}

test("Docker source selection binds the actual event and explicit release run", () => {
  assert.equal(consumer.eventRun("workflow_run", { workflow_run: { id: 10, conclusion: "success" } }, "10"), 10);
  assert.equal(consumer.eventRun("workflow_dispatch", { inputs: { release_run: "10" } }, "10"), 10);
  for (const status of ["failure", "cancelled", "skipped", null]) assert.throws(() => consumer.eventRun("workflow_run", { workflow_run: { id: 10, conclusion: status } }));
  for (const value of ["", "0", "-1", "1.1", "1\nunsafe=1", "9007199254740992"]) assert.throws(() => consumer.eventRun("workflow_dispatch", { inputs: { release_run: value } }));
  assert.throws(() => consumer.eventRun("workflow_run", { workflow_run: { id: 10, conclusion: "success" } }, "11"));
  assert.throws(() => consumer.eventRun("push", {}));
});
test("Docker has no supplied authority or public proof override", async () => {
  await assert.rejects(consumer.authenticated({ publicationAuthorized: true }), /No supplied proof/);
  const code = text("scripts/docker-from-release.cjs");
  assert.match(code, /require\("\.\/main-admission\.cjs"\)\.createPublicReads\(\)/);
  assert.match(code, /proof\.verifyPublishedSource\(input, reads\)/);
  assert.doesNotMatch(code, /BAMBOO_RELEASE_SIGNING_KEY|makeFrozenSourceContext|saveReceipt|createLifecycle/);
});
test("Docker child environment excludes publisher and workflow write credentials", t => {
  const blocked = ["GH_TOKEN", "GITHUB_TOKEN", "GITHUB_OUTPUT", "GITHUB_ENV", "NODE_OPTIONS", "NODE_PATH", "CARGO_REGISTRY_TOKEN", "GITHUB_RUNTIME_TOKEN"];
  const previous = Object.fromEntries(blocked.map(key => [key, process.env[key]]));
  for (const key of blocked) process.env[key] = "fixture-not-a-credential";
  t.after(() => { for (const [key, value] of Object.entries(previous)) { if (value === undefined) delete process.env[key]; else process.env[key] = value; } });
  for (const key of blocked) assert.equal(Object.hasOwn(consumer.safeEnv(), key), false);
  assert.ok(Object.keys(consumer.safeEnv()).every(key => ["PATH", "HOME", "TMPDIR", "LANG", "LC_ALL", "SystemRoot"].includes(key)));
});
function locatorJob(id, attempt, conclusion = "success") {
  return { id, run_attempt: attempt, name: "Publish and verify complete executable assets", status: "completed", conclusion,
    started_at: "2026-10-27T00:00:00Z", completed_at: "2026-10-27T00:00:01Z",
    steps: [{ number: 1, name: "Finalize", status: "completed", conclusion: "success", started_at: "2026-10-27T00:00:00Z", completed_at: "2026-10-27T00:00:01Z" }] };
}
test("Docker locator reads original physical log, then delegates all authority", async () => {
  const proof = require("./published-source-admission.cjs"), jobs = [locatorJob(101, 1), locatorJob(202, 2)], logs = [];
  const frame = { schemaVersion: 1, kind: proof.KIND, publicationAuthorized: false,
    repository: { id: 1164779771, fullName: "bigduu/Bamboo-agent" }, source: { revision: facts.source.revision, featurePr: 42, featureRunId: 43 },
    controller: { runId: 10 }, release: {}, frontend: {}, crates: {}, native: {}, jobs: [] };
  const line = "2026-10-27T00:00:01Z " + proof.PREFIX + Buffer.from(core.canonicalJson(frame)).toString("base64");
  const reads = { json: async () => ({ total_count: 2, jobs }), jobLog: async id => { logs.push(id); return line; } };
  assert.deepEqual(await consumer.locate(reads, 10), { sourceRevision: facts.source.revision, featurePr: 42, ciRun: 43, releaseRun: 10 });
  assert.deepEqual(logs, [101]); // copied ID202 never becomes the original execution
  for (const rows of [[locatorJob(101, 1), locatorJob(202, 2, "failure")], [locatorJob(101, 1), locatorJob(101, 2)]]) {
    await assert.rejects(consumer.locate({ ...reads, json: async () => ({ total_count: 2, jobs: rows }) }, 10));
  }
  await assert.rejects(consumer.locate({ ...reads, json: async () => ({ jobs }) }, 10));
  await assert.rejects(consumer.locate({ ...reads, json: async () => ({ total_count: 3, jobs }) }, 10), /Incomplete/);
  await assert.rejects(consumer.locate({ ...reads, jobLog: async () => line + "\n" + line }, 10), /duplicate/);
});
test("Docker preserves both architectures and explicitly blocks missing ARM originals", () => {
  assert.deepEqual(consumer.PLATFORMS, ["linux/amd64", "linux/arm64"]);
  assert.equal(consumer.target("linux/amd64"), "x86_64-unknown-linux-gnu");
  assert.throws(() => consumer.target("linux/arm64"), /#2353/);
  assert.throws(() => consumer.target("linux/386"), /Unknown Docker architecture/);
});
test("Docker original bytes require both exact size and SHA", () => {
  const bytes = Buffer.from("original"); const ref = { sizeBytes: bytes.length, sha256: core.sha256(bytes) };
  assert.equal(consumer.verifyBytes(bytes, ref), bytes);
  assert.throws(() => consumer.verifyBytes(Buffer.from("tampered"), ref));
  assert.throws(() => consumer.verifyBytes(bytes, { ...ref, sizeBytes: bytes.length + 1 }));
  assert.throws(() => consumer.verifyBytes(bytes, { ...ref, sha256: "0".repeat(64) }));
});
test("Docker original asset readback rejects changed identity and bytes", async () => {
  const bytes = Buffer.from("original"), ref = { assetId: 7, name: "original", sizeBytes: bytes.length, sha256: core.sha256(bytes) };
  const value = { id: 7, name: "original", size: bytes.length, state: "uploaded", digest: "sha256:" + ref.sha256 }; let calls = 0;
  const reads = { json: async route => { assert.equal(route, "releases/assets/7"); calls++; return { ...value, download_count: calls }; }, assetBytes: async (id, maximum) => { assert.equal(id, 7); assert.equal(maximum, bytes.length); return bytes; } };
  assert.deepEqual(await consumer.original(reads, ref), bytes); assert.equal(calls, 2);
  await assert.rejects(consumer.original({ ...reads, assetBytes: async () => Buffer.from("tampered") }, ref));
  let changed = 0; await assert.rejects(consumer.original({ ...reads, json: async () => ++changed === 1 ? value : { ...value, size: value.size + 1 } }, ref));
});
test("Docker tiny real Host tar roundtrip preserves every original byte", () => {
  const files = host(); assert.deepEqual(consumer.hostFiles(tar(entries(files)), manifest(files)), files);
});
for (const variant of ["extra", "traversal", "absolute", "symlink", "duplicate", "missing", "hash", "manifest", "dependency"]) {
  test("Docker Host archive rejects " + variant, () => {
    const files = host(), rows = entries(files); let expected = manifest(files);
    if (variant === "extra") rows.push({ name: "browser-runtime/extra.cjs", bytes: "eA==" });
    if (variant === "traversal") rows[0].name = "../host.cjs";
    if (variant === "absolute") rows[0].name = "/browser-runtime/host.cjs";
    if (variant === "symlink") rows[0] = { name: rows[0].name, link: "/tmp/foreign-host" };
    if (variant === "duplicate") rows.push(rows[0]);
    if (variant === "missing") rows.pop();
    if (variant === "hash") expected[0].sha256 = "0".repeat(64);
    if (variant === "manifest") expected = expected.slice(1);
    if (variant === "dependency") {
      const pkg = JSON.parse(files["browser-runtime/package.json"]); pkg.dependencies["playwright-core"] = "latest";
      files["browser-runtime/package.json"] = Buffer.from(JSON.stringify(pkg));
      rows.splice(0, rows.length, ...entries(files)); expected = manifest(files);
    }
    assert.throws(() => consumer.hostFiles(tar(rows), expected));
  });
}
const report = `Class: ELF64\nMachine: Advanced Micro Devices X86-64\n0 (NEEDED) Shared library: [libc.so.6]\nGLIBC_2.35 GLIBCXX_3.4.29 CXXABI_1.3.13\n`;
test("Docker ELF inspection records ABI requirements, without claiming compatibility", () => {
  assert.deepEqual(consumer.elfReport(report, "linux/amd64"), { needed: ["libc.so.6"], symbolVersions: ["CXXABI_1.3.13", "GLIBCXX_3.4.29", "GLIBC_2.35"] });
  assert.throws(() => consumer.elfReport(report.replace("GLIBC_2.35", "GLIBC_2.39"), "linux/amd64"), /newer glibc/);
  assert.throws(() => consumer.elfReport(report, "linux/arm64"));
  assert.throws(() => consumer.elfReport(report.replace("libc.so.6", "missing"), "linux/amd64"));
  assert.throws(() => consumer.elfReport(report.replace("ELF64", "ELF32"), "linux/amd64"));
});
test("Docker smoke uses the original version, loader, API, frontend and one Host dependency launch", () => {
  const commands = consumer.smokeCommands("ghcr.io/bigduu/bamboo:candidate-1-1-amd64", facts.release.version);
  assert.equal(commands.length, 4);
  for (const args of commands) { assert.deepEqual(args.slice(0, 10), ["run", "--rm", "--init", "--network", "none", "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--entrypoint"]); assert.ok(!args.includes("-v")); }
  assert.equal(commands[0].at(-1), "--version");
  assert.match(commands[1].at(-1), /ldd.*not found/);
  assert.match(commands[1].at(-1), /node --version \| grep -Eq '\^v24/);
  assert.match(commands[2].at(-1), /chromium\.launch.*b\.close/);
  assert.match(commands[3].at(-1), /api\/v1\/health[\s\S]*content-type:[\s\S]*\.frontend-manifest\.json/);
  for (const i of [1, 3]) {
    const parsed = spawnSync("sh", ["-n"], { input: commands[i].at(-1), encoding: "utf8", env: consumer.safeEnv() });
    assert.equal(parsed.status, 0, parsed.stderr); // parse only; no container or product execution
  }
  assert.doesNotMatch(commands.flat().join(" "), /cargo|npm|vitest|test:runtime|--env|--volume/);
  for (const image of ["-evil:tag", "bamboo:tag;echo bad", "bamboo:tag\n", "bamboo", "https://foreign/x:tag"]) assert.throws(() => consumer.imageReference(image));
});
test("Docker digest carriers are per run attempt and architecture", () => {
  assert.equal(consumer.candidateImage(4, 2, "linux/amd64"), "ghcr.io/bigduu/bamboo:candidate-4-2-amd64");
  assert.notEqual(consumer.digestFile(4, 1, "linux/amd64"), consumer.digestFile(4, 2, "linux/amd64"));
  assert.notEqual(consumer.digestFile(4, 2, "linux/amd64"), consumer.digestFile(4, 2, "linux/arm64"));
  assert.deepEqual(consumer.digestRecord(record(), facts), record());
  for (const key of ["source", "version", "releaseRun", "publisher", "releaseId", "platform", "digest"]) assert.throws(() => consumer.digestRecord({ ...record(), [key]: "foreign" }, facts));
  assert.throws(() => consumer.digestRecord({ ...record(), publicationAuthorized: true }, facts));
});
function withDigests(t, rows) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "bamboo2320-digests-")); fs.mkdirSync(path.join(directory, "docker-digests"));
  const previous = { RUNNER_TEMP: process.env.RUNNER_TEMP, GITHUB_RUN_ID: process.env.GITHUB_RUN_ID };
  process.env.RUNNER_TEMP = directory; process.env.GITHUB_RUN_ID = "4";
  t.after(() => { for (const [key, value] of Object.entries(previous)) { if (value === undefined) delete process.env[key]; else process.env[key] = value; } fs.rmSync(directory, { recursive: true }); });
  for (const [name, value] of rows) fs.writeFileSync(path.join(directory, "docker-digests", name), JSON.stringify(value));
  return directory;
}
test("Docker reads both exact same-release architecture digests across failed-job attempts", t => {
  withDigests(t, [[consumer.digestFile(4, 1, "linux/amd64"), record()], [consumer.digestFile(4, 2, "linux/arm64"), record("linux/arm64")]]);
  assert.equal(consumer.readDigests(facts).length, 2);
});
for (const variant of ["missing", "foreign", "duplicate", "symlink", "wrong-run"]) {
  test("Docker manifest refuses " + variant + " digest evidence", t => {
    const rows = [[consumer.digestFile(4, 1, "linux/amd64"), record()]];
    if (variant !== "missing") rows.push([consumer.digestFile(4, 1, "linux/arm64"), { ...record("linux/arm64"), ...(variant === "foreign" ? { version: "2026.1.1" } : {}) }]);
    if (variant === "duplicate") rows.push([consumer.digestFile(4, 2, "linux/amd64"), record()]);
    if (variant === "wrong-run") rows[0][0] = consumer.digestFile(5, 1, "linux/amd64");
    const directory = withDigests(t, rows);
    if (variant === "symlink") { const file = path.join(directory, "docker-digests", rows[0][0]); fs.renameSync(file, path.join(directory, "foreign.json")); fs.symlinkSync(path.join(directory, "foreign.json"), file); }
    assert.throws(() => consumer.readDigests(facts));
  });
}
test("Docker multi-arch readback binds platform descriptors and exact input digests", () => {
  const records = [record(), record("linux/arm64")]; const manifest = { digest: digest("3"), manifests: records.map(value => ({ digest: value.digest, platform: { os: "linux", architecture: value.platform.slice(6) } })) };
  assert.equal(consumer.manifestReport(manifest, records), digest("3"));
  assert.throws(() => consumer.manifestReport({ ...manifest, manifests: manifest.manifests.slice(1) }, records));
  const wrong = structuredClone(manifest); wrong.manifests[0].digest = digest("4"); assert.throws(() => consumer.manifestReport(wrong, records));
});
test("Docker workflow has one runtime consumer, no source or frontend rebuild", () => {
  const workflow = text(".github/workflows/docker-publish.yml"), docker = text("docker/Dockerfile"), compose = text("docker/docker-compose.yml");
  assert.match(workflow, /workflow_run:\n    workflows: \[Publish Crate\]\n    types: \[completed\]/);
  assert.doesNotMatch(workflow, /^  (?:push|pull_request):|frontend_package:|lotus_version:|\bnpm\b|\bcargo\b|context: \./m);
  assert.match(workflow, /needs: \[source, image\]/); assert.match(workflow, /fail-fast: false/);
  for (const platform of consumer.PLATFORMS) assert.equal(workflow.split("- platform: " + platform).length, 2);
  assert.ok(workflow.indexOf(".cjs smoke") < workflow.indexOf("uses: docker/login-action"));
  assert.ok(workflow.indexOf(".cjs smoke") < workflow.indexOf(".cjs push"));
  assert.match(workflow, /load: true\n          push: false/); assert.match(workflow, /retention-days: 90/);
  assert.match(workflow, /pattern: docker-digest-\$\{\{ github.run_id \}\}-\*/);
  assert.match(workflow, /image-ref: \$\{\{ needs.merge.outputs.image \}\}/);
  assert.match(workflow, /trivy-action@ed142fd0673e97e23eac54620cfb913e5ce36c25/); assert.match(workflow, /exit-code: "0"/);
  assert.match(docker, new RegExp("FROM " + consumer.RUNTIME.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")));
  const instructions = docker.split("\n").filter(line => !line.trimStart().startsWith("#")).join("\n");
  assert.equal((docker.match(/^FROM /gm) || []).length, 1); assert.doesNotMatch(instructions, /cargo|rust:|COPY \.|--from=|npm (?:install|run)|\bvite\b/i);
  assert.match(docker, /npm ci --omit=dev --ignore-scripts/); assert.match(docker, /install --with-deps --only-shell chromium/);
  for (const file of ["browser-runtime/host.cjs", "bamboo", "originals.json"]) assert.ok(docker.includes("COPY " + file));
  assert.match(docker, /--uid 10001/); assert.match(docker, /USER bamboo/); assert.match(docker, /ENTRYPOINT \["bamboo"\]/);
  assert.match(docker, /CMD \["serve", "--port", "9562", "--bind", "0.0.0.0", "--data-dir", "\/data"\]/);
  assert.doesNotMatch(compose, /^    build:/m); assert.match(compose, /BAMBOO_IMAGE:\?/); assert.match(compose, /127\.0\.0\.1:9562:9562/); assert.match(compose, /pids_limit: 512/);
  const owner = text("scripts/ci-policy.test.cjs"); assert.equal((owner.match(/^require\("\.\/docker-from-release.test.cjs"\)/gm) || []).length, 1);
});
