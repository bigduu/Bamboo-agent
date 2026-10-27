"use strict";
// Data/transport/static-routing fixtures only: no Docker, Cargo or browser run.
const assert = require("node:assert/strict"), test = require("node:test");
const fs = require("node:fs"), path = require("node:path"), vm = require("node:vm");
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

test("Docker Source2361 canonical handoff rejects locator-only, duplicate and oversized evidence", () => {
  const value = { schemaVersion: 1, kind: "bamboo-docker-source-proof/v1", publicationAuthorized: false,
    mode: "full", selection: { ...record(), consumerRun: 50, controller: "d".repeat(40) },
    job: { id: 501, attempt: 1 }, origin: { id: 501, attempt: 1 }, data: {} };
  delete value.selection.platform; delete value.selection.digest;
  const line = item => at(1) + " " + consumer.SOURCE_PREFIX + Buffer.from(core.canonicalJson(item)).toString("base64");
  assert.deepEqual(consumer.parseSourceFrame(line(value)), value);
  for (const log of [line(value) + "\n" + line(value), line(value.selection), line({ ...value, publicationAuthorized: true }),
    line({ ...value, data: { oversized: "x".repeat(65536) } })]) assert.throws(() => consumer.parseSourceFrame(log));
});

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
test("Docker preserves standard native targets and maps the separate ARM original", () => {
  assert.deepEqual(consumer.PLATFORMS, ["linux/amd64", "linux/arm64"]);
  assert.equal(consumer.target("linux/amd64"), "x86_64-unknown-linux-gnu");
  assert.equal(consumer.target("linux/arm64"), "aarch64-unknown-linux-gnu");
  assert.deepEqual(require("./sidecar-release.cjs").TARGETS,
    ["aarch64-apple-darwin", "x86_64-pc-windows-msvc", "x86_64-unknown-linux-gnu"]);
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
test("Docker Host accepts the complete bounded archive after base64 expansion", () => {
  const files = host(), maximum = require("./sidecar-release.cjs").MAX_HOST;
  const others = Object.entries(files).filter(([name]) => name !== "browser-runtime/host.cjs").reduce((size, [,bytes]) => size + bytes.length, 0);
  files["browser-runtime/host.cjs"] = Buffer.alloc(maximum - others, 32); // compressible data; never executed
  assert.deepEqual(consumer.hostFiles(tar(entries(files)), manifest(files)), files);
  files["browser-runtime/host.cjs"] = Buffer.alloc(maximum - others + 1, 32);
  assert.throws(() => consumer.hostFiles(tar(entries(files)), manifest(files)));
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
// Recovery fixtures use real tiny ZIP bytes and inert official-read ports only.
const at = second => new Date(Date.UTC(2026, 9, 27, 0, 0, second)).toISOString();
function uploadLog(artifact) {
  return at(7) + ` Artifact ${artifact.name} has been successfully uploaded! Final size is ${artifact.size_in_bytes} bytes. Artifact ID is ${artifact.id}\n` +
    at(7) + ` Artifact download URL: https://github.com/bigduu/Bamboo-agent/actions/runs/50/artifacts/${artifact.id}\n`;
}
function imageJob(id = 101, attempt = 1, build = "success", reuse = "skipped") {
  const names = [consumer.SELECT, "Copy authenticated binary and Host originals", consumer.BUILD, consumer.SMOKE, consumer.REUSE, consumer.PUSH, consumer.UPLOAD];
  return { id, run_id: 50, run_attempt: attempt, head_sha: "e".repeat(40), head_branch: "main", name: "Repackage originals (linux/amd64)", status: "completed", conclusion: "success", started_at: at(0), completed_at: at(20),
    steps: names.map((name,i) => ({ name, number: i + 1, status: "completed", conclusion: name === consumer.REUSE ? reuse : name === consumer.SELECT ? "success" : build === "success" ? "success" : name === consumer.BUILD ? build : "skipped", started_at: at(i + 1), completed_at: at(i + 2) })) };
}
function zipDigest(name, value, variant = "") {
  const program = `import io,json,stat,sys,zipfile
item=json.load(sys.stdin); out=io.BytesIO()
with zipfile.ZipFile(out,'w',zipfile.ZIP_DEFLATED) as archive:
 info=zipfile.ZipInfo(item['name']); info.external_attr=(stat.S_IFLNK|0o777)<<16 if item['variant']=='symlink' else (stat.S_IFREG|0o644)<<16
 archive.writestr(info,json.dumps(item['value']))
 if item['variant']=='extra': archive.writestr('extra.json','{}')
sys.stdout.buffer.write(out.getvalue())`;
  const result = spawnSync("python3", ["-I", "-c", program], { input: JSON.stringify({ name, value, variant }), env: consumer.safeEnv(), timeout: 10000 }); assert.equal(result.status, 0); return result.stdout;
}
function carrierFixture() {
  const run = { id: 50, run_attempt: 2, workflow_id: 70, head_sha: "e".repeat(40), head_branch: "main", path: ".github/workflows/docker-publish.yml", event: "workflow_run", status: "in_progress", created_at: at(0), check_suite_id: 80, check_suite_node_id: "CS50", repository: { id: 1164779771, full_name: "bigduu/Bamboo-agent" }, head_repository: { id: 1164779771, full_name: "bigduu/Bamboo-agent" } };
  const physical = imageJob(), raw = zipDigest(consumer.digestFile(50, 1, "linux/amd64"), record());
  const artifact = { id: 900, name: "docker-digest-50-1-0", expired: false, size_in_bytes: raw.length, digest: "sha256:" + core.sha256(raw), created_at: at(7), workflow_run: { id: 50, head_sha: run.head_sha, head_branch: "main" } };
  const fixture = { run, physical, raw, artifact, jobs: [physical], artifacts: [artifact], getCalls: [], byteCalls: 0, log: uploadLog(artifact), logCalls: [] };
  const get = async route => { fixture.getCalls.push(route); if (route === "actions/jobs/101") return fixture.physical; if (route === "actions/artifacts/900") return fixture.artifact; throw Error("unexpected read " + route); };
  fixture.context = { run, get, paged: async () => fixture.artifacts };
  fixture.state = { controller: "d".repeat(40), facts, input: { releaseRun: 10 }, reads: { jobLog: async id => { fixture.logCalls.push(id); return fixture.log; }, artifactBytes: async (id, maximum) => { assert.equal(id, 900); assert.equal(maximum, fixture.artifact.size_in_bytes); fixture.byteCalls++; return fixture.raw; } } };
  return fixture;
}
test("Docker recovery projects copied success and reuses a physical original after a full rerun", () => {
  const first = imageJob(), copied = structuredClone(first); copied.id = 202; copied.run_attempt = 2;
  assert.equal(consumer.imageHistory([first,copied], { id: 50, run_attempt: 2, head_sha: first.head_sha, head_branch: "main" }, "linux/amd64", 3, true).physical.id, 101);
  const reused = imageJob(203, 2, "skipped", "success");
  assert.equal(consumer.imageHistory([first,reused], { id: 50, run_attempt: 2, head_sha: first.head_sha, head_branch: "main" }, "linux/amd64", 3, true).physical.id, 101);
});
test("Docker recovery retries only a known unfinished target before any original or write", () => {
  for (const build of ["failure", "skipped"]) {
    const job = imageJob(101, 1, build); job.conclusion = "failure";
    assert.equal(consumer.imageHistory([job], { id: 50, run_attempt: 2, head_sha: job.head_sha, head_branch: "main" }, "linux/amd64", 2).physical, undefined);
  }
});
test("Docker recovery never rebuilds after a reuse path loses its physical origin", () => {
  for (const outcome of ["success", "failure"]) {
    const job = imageJob(202,2,"skipped",outcome); job.conclusion = outcome;
    assert.throws(() => consumer.imageHistory([job], { id: 50, run_attempt: 3, head_sha: job.head_sha, head_branch: "main" }, "linux/amd64", 3), /physical/);
  }
});
test("Docker recovery never treats an orphan or expired carrier as a new target", async () => {
  const context = { run: { id: 50 }, paged: async () => [{ id: 900, name: "docker-digest-50-1-0", expired: true }] };
  await assert.rejects(consumer.noPriorCarrier(context,"linux/amd64"), /carrier/);
  await consumer.noPriorCarrier(context,"linux/arm64");
  context.paged = async () => []; await consumer.noPriorCarrier(context,"linux/amd64");
});
for (const variant of ["missing-step", "cancelled", "failed-owner", "failed-upload", "second-assembly", "latest-failed", "fake-green-reuse", "foreign-header", "duplicate-attempt"]) {
  test("Docker recovery stops " + variant + " image history", () => {
    const first = imageJob(), run = { id: 50, run_attempt: 2, head_sha: first.head_sha, head_branch: "main" }; let jobs = [first];
    if (variant === "missing-step") first.steps.pop();
    if (variant === "cancelled") { first.steps.find(s => s.name === consumer.BUILD).conclusion = "cancelled"; first.conclusion = "cancelled"; }
    if (variant === "failed-owner") first.conclusion = "failure";
    if (variant === "failed-upload") first.steps.find(s => s.name === consumer.UPLOAD).conclusion = "failure";
    if (variant === "second-assembly") { const second = imageJob(202, 2); second.started_at = at(30); second.completed_at = at(50); second.steps.forEach((s,i) => { s.started_at = at(31 + i); s.completed_at = at(32 + i); }); jobs.push(second); }
    if (variant === "latest-failed") { const second = imageJob(202, 2, "skipped"); second.conclusion = "failure"; jobs.push(second); }
    if (variant === "fake-green-reuse") jobs.push(imageJob(202, 2, "skipped"));
    if (variant === "foreign-header") first.head_sha = "f".repeat(40);
    if (variant === "duplicate-attempt") jobs.push(imageJob(202, 1));
    assert.throws(() => consumer.imageHistory(jobs, run, "linux/amd64", 3, true));
  });
}
test("Docker recovery reads the original raw Actions ZIP once with fresh metadata and job proof", async () => {
  const f = carrierFixture(); assert.deepEqual(await consumer.readCarrier(f.state, f.context, "linux/amd64", f.physical), record()); assert.equal(f.byteCalls, 1);
  assert.deepEqual(f.logCalls, [101]);
  assert.deepEqual(f.getCalls, ["actions/jobs/101", "actions/artifacts/900", "actions/jobs/101"]);
});
test("Docker physical upload log binds the unique original ID, URL, size and step window", async () => {
  for (const variant of ["wrong-id", "wrong-url", "wrong-size", "duplicate", "duplicate-url", "outside-record", "outside-url", "missing"]) {
    const f = carrierFixture();
    if (variant === "wrong-id") f.log = f.log.replace("Artifact ID is 900", "Artifact ID is 901");
    if (variant === "wrong-url") f.log = f.log.replace("/artifacts/900", "/artifacts/901");
    if (variant === "wrong-size") f.log = f.log.replace(`Final size is ${f.raw.length}`, `Final size is ${f.raw.length + 1}`);
    if (variant === "duplicate") f.log += f.log.split("\n")[0] + "\n";
    if (variant === "duplicate-url") f.log += f.log.split("\n")[1] + "\n";
    if (variant === "outside-record") f.log = f.log.replace(at(7), at(30));
    if (variant === "outside-url") f.log = f.log.replace(at(7) + " Artifact download", at(30) + " Artifact download");
    if (variant === "missing") f.log = "";
    await assert.rejects(consumer.readCarrier(f.state, f.context, "linux/amd64", f.physical), undefined, variant);
    assert.equal(f.byteCalls, 0, variant + " must stop before reading a carrier");
  }
});
for (const variant of ["missing", "expired", "duplicate", "foreign-run", "tampered-zip", "wrong-window", "changed-asset", "changed-attempt", "wrong-file", "symlink", "extra", "foreign-source"]) {
  test("Docker recovery rejects " + variant + " immutable carrier", async () => {
    const f = carrierFixture();
    if (variant === "missing") f.artifacts = [];
    if (variant === "expired") f.artifact.expired = true;
    if (variant === "duplicate") f.artifacts.push({ ...f.artifact, id: 901 });
    if (variant === "foreign-run") f.artifact.workflow_run.id = 51;
    if (variant === "tampered-zip") f.raw = Buffer.from("tampered");
    if (variant === "wrong-window") f.artifact.created_at = at(30);
    if (variant === "changed-asset") f.context.get = async route => route.startsWith("actions/jobs/") ? f.physical : { ...f.artifact, digest: digest("9") };
    if (variant === "changed-attempt") { let count = 0; f.context.get = async route => route.startsWith("actions/jobs/") ? ++count === 1 ? f.physical : { ...f.physical, run_attempt: 2 } : f.artifact; }
    if (["wrong-file", "symlink", "extra", "foreign-source"].includes(variant)) {
      f.raw = zipDigest(variant === "wrong-file" ? "../foreign.json" : consumer.digestFile(50,1,"linux/amd64"), variant === "foreign-source" ? { ...record(), source: "f".repeat(40) } : record(), variant);
      f.artifact.size_in_bytes = f.raw.length; f.artifact.digest = "sha256:" + core.sha256(f.raw);
    }
    await assert.rejects(consumer.readCarrier(f.state, f.context, "linux/amd64", f.physical));
  });
}
function suite(run, D = run.head_sha) {
  return { databaseId: run.check_suite_id, repository: { databaseId: 1164779771, nameWithOwner: "bigduu/Bamboo-agent" }, app: { databaseId: 15368, slug: "github-actions" }, commit: { oid: run.head_sha }, workflowRun: { databaseId: run.id, event: run.event, runAttempt: run.run_attempt, workflow: { databaseId: run.workflow_id }, file: { path: ".github/workflows/docker-publish.yml", repositoryName: "bigduu/Bamboo-agent", repositoryFileUrl: `https://github.com/bigduu/Bamboo-agent/blob/${D}/.github/workflows/docker-publish.yml` } } };
}
function sourceJob(run) { return { ...imageJob(301,1), run_id: run.id, head_sha: run.head_sha, name: consumer.SOURCE_JOB, steps: [{ name: consumer.SOURCE_STEP, number: 1, status: "completed", conclusion: "success", started_at: at(0), completed_at: at(2) }] }; }
test("Docker recovery cross-run selector blocks the same formal output before any new assembly", async () => {
  for (const variant of ["same-source", "unknown", "different-D", "foreign-suite", "incomplete-run", "empty-completed", "same-version-other-publisher"]) {
    const f = carrierFixture(), prior = { ...f.run, id: 40, run_attempt: 1, status: "completed", head_sha: f.state.controller }; let jobs = [sourceJob(prior)];
    const other = { ...f.state, facts: { ...facts, source: { revision: "c".repeat(40) }, release: { version: "2026.10.28", id: 31 }, controller: { ...facts.controller, runId: 11 } } };
    const locator = variant === "same-source" ? f.state : variant === "same-version-other-publisher" ? { ...f.state, facts: { ...facts, controller: { ...facts.controller, runId: 11 } } } : other;
    if (variant === "unknown") jobs = [{ ...imageJob(), run_id: prior.id, head_sha: prior.head_sha }];
    if (variant === "different-D") locator.controller = "f".repeat(40);
    if (variant === "incomplete-run") { prior.status = "in_progress"; jobs = []; }
    if (variant === "empty-completed") { prior.conclusion = "success"; jobs = []; }
    f.state.reads.jobLog = async () => at(1) + " " + consumer.sourceLine(locator,40);
    f.state.reads.checkSuite = async () => { const value = suite(prior); if (variant === "foreign-suite") value.repository.databaseId = 1; return value; };
    const context = { run: f.run, get: async () => ({ id: 10, created_at: at(0) }), paged: async route => route.includes("/runs?") ? [f.run,prior] : jobs };
    await assert.rejects(consumer.crossRunGuard(f.state, context), undefined, variant);
  }
});
test("Docker recovery cross-run selector ignores only proven other-source execution or unexecuted runs", async () => {
  const f = carrierFixture(), prior = { ...f.run, id: 40, run_attempt: 1, status: "completed", head_sha: f.state.controller }, job = sourceJob(prior);
  const other = { ...f.state, facts: { ...facts, source: { revision: "c".repeat(40) }, release: { version: "2026.10.28", id: 31 }, controller: { ...facts.controller, runId: 11 } } };
  f.state.reads.jobLog = async () => at(1) + " " + consumer.sourceLine(other,40); f.state.reads.checkSuite = async () => suite(prior);
  const context = { run: f.run, get: async () => ({ id: 10, created_at: at(0) }), paged: async route => route.includes("/runs?") ? [prior] : [job] };
  await consumer.crossRunGuard(f.state, context);
  context.paged = async route => route.includes("/runs?") ? [prior] : [{ ...job, status: "completed", conclusion: "skipped", steps: null }]; await consumer.crossRunGuard(f.state, context);
});
test("Docker recovery cross-run selector ignores later queued duplicate consumers", async () => {
  const f = carrierFixture(), later = { ...f.run, id: 51, status: "queued" };
  const context = { run: f.run, get: async () => ({ id: 10, created_at: at(0) }), paged: async route => { assert.ok(route.includes("/runs?")); return [f.run,later]; } };
  await consumer.crossRunGuard(f.state, context);
});
test("Docker recovery rechecks the live consumer attempt and actual write step before a registry command", async () => {
  const f = carrierFixture(), current = imageJob(202,2); current.status = "in_progress"; current.steps.find(s => s.name === consumer.PUSH).status = "in_progress";
  const context = { run: f.run, current, get: async route => route === "actions/runs/50" ? f.run : current };
  await consumer.freshConsumer(context,consumer.PUSH);
  context.get = async route => route === "actions/runs/50" ? { ...f.run, run_attempt: 3 } : current; await assert.rejects(consumer.freshConsumer(context,consumer.PUSH));
  context.get = async route => route === "actions/runs/50" ? f.run : { ...current, status: "completed", conclusion: "cancelled" }; await assert.rejects(consumer.freshConsumer(context,consumer.PUSH));
  context.get = async route => route === "actions/runs/50" ? f.run : current; await assert.rejects(consumer.freshConsumer(context,consumer.BUILD));
});
test("Docker recovery consumer API identity keeps metadata X distinct from executed D and rejects phase spoofing", async t => {
  const f = carrierFixture(), current = imageJob(202,2); current.status = "in_progress";
  const previous = { GITHUB_RUN_ID: process.env.GITHUB_RUN_ID, GITHUB_RUN_ATTEMPT: process.env.GITHUB_RUN_ATTEMPT, GITHUB_JOB: process.env.GITHUB_JOB, GITHUB_EVENT_NAME: process.env.GITHUB_EVENT_NAME, DOCKER_PLATFORM: process.env.DOCKER_PLATFORM };
  Object.assign(process.env, { GITHUB_RUN_ID: "50", GITHUB_RUN_ATTEMPT: "2", GITHUB_JOB: "image", GITHUB_EVENT_NAME: "workflow_run", DOCKER_PLATFORM: "linux/amd64" });
  t.after(() => { for (const [key,value] of Object.entries(previous)) { if (value === undefined) delete process.env[key]; else process.env[key] = value; } });
  f.state.reads.json = async route => route === "actions/runs/50" ? f.run : route === "actions/workflows/70" ? { id: 70, path: f.run.path } : { total_count: 2, jobs: [f.physical,current] };
  f.state.reads.checkSuite = async () => suite(f.run);
  assert.notEqual(f.run.head_sha, f.state.controller); assert.equal((await consumer.consumerExecution(f.state,"image")).current.id, 202);
  process.env.GITHUB_JOB = "arm-originals"; current.name = consumer.ARM_JOB;
  assert.equal((await consumer.consumerExecution(f.state,"arm-originals")).current.id, 202);
  await assert.rejects(consumer.consumerExecution(f.state,"supplied-recipe"), /Unknown Docker job role/);
  process.env.GITHUB_JOB = "image"; current.name = "Repackage originals (linux/amd64)";
  await assert.rejects(consumer.consumerExecution(f.state,"merge"), /Actual job key/);
  f.state.reads.checkSuite = async () => ({ ...suite(f.run), app: { databaseId: 1, slug: "foreign" } }); await assert.rejects(consumer.consumerExecution(f.state,"image"));
  f.state.reads.checkSuite = async () => suite(f.run); const previousRead = f.state.reads.json;
  f.state.reads.json = async route => route.includes("/jobs?") ? { jobs: [f.physical,current] } : previousRead(route); await assert.rejects(consumer.consumerExecution(f.state,"image"), /total_count/);
});
test("Docker fresh publication proof after carrier reads stops registry writes on changed Source identity", async () => {
  for(const variant of ["source","release","controller","completion","proof-rejected","latest-advanced","stable"]){
    const f=await source2361Fixture();await f.seal();f.prepareDigests();let changed=false;
    f.hook=(key,args,value)=>{if(!changed&&key==="artifactBytes"&&args[0]===9001){changed=true;
      if(variant==="source")f.f.f.release.commit.sha="f".repeat(40);
      if(variant==="release")f.f.state.release.id++;
      if(variant==="controller")f.f.state.run.head_sha="f".repeat(40);
      if(variant==="completion")f.f.state.record.source.tree="f".repeat(40);
      if(variant==="proof-rejected")f.f.f.newerRun=true;
      if(variant==="latest-advanced")Object.assign(f.latest,{id:4001,tag_name:"v2026.10.28",target_commitish:"e".repeat(40)});
    }return value;};
    const c=f.load("merge",undefined,"publish");
    if(["stable","latest-advanced"].includes(variant)){
      await c.__main();assert.equal(f.writes,1);
      assert.deepEqual(f.writtenTags,[consumer.IMAGE+":v2026.10.27",...(variant==="stable"?[consumer.IMAGE+":latest"]:[])]);
    }else{await assert.rejects(c.__main());assert.equal(f.writes,0,variant);}
    assert.equal(changed,true);assert.equal(f.full,1,"Reuse one independently authenticated full Source proof");
  }
});
test("Docker multi-arch readback binds platform descriptors and exact input digests", () => {
  const records = [record(), record("linux/arm64")]; const manifest = { digest: digest("3"), manifests: records.map(value => ({ digest: value.digest, platform: { os: "linux", architecture: value.platform.slice(6) } })) };
  assert.equal(consumer.manifestReport(manifest, records), digest("3"));
  assert.throws(() => consumer.manifestReport({ ...manifest, manifests: manifest.manifests.slice(1) }, records));
  const wrong = structuredClone(manifest); wrong.manifests[0].digest = digest("4"); assert.throws(() => consumer.manifestReport(wrong, records));
});
test("Docker historical recovery writes only its version unless the official latest identity matches", () => {
  const latest = { id: 30, tag_name: "v2026.10.27", target_commitish: facts.source.revision, draft: false, prerelease: false };
  assert.deepEqual(consumer.publicationTags(facts, latest), [consumer.IMAGE + ":v2026.10.27", consumer.IMAGE + ":latest"]);
  assert.deepEqual(consumer.publicationTags(facts, { ...latest, id: 31, tag_name: "v2026.10.28", target_commitish: "c".repeat(40) }), [consumer.IMAGE + ":v2026.10.27"]);
  for (const change of [{ tag_name: "v2026.10.28" }, { target_commitish: "c".repeat(40) }, { draft: true }, { prerelease: true }, { id: 31 }]) {
    assert.throws(() => consumer.publicationTags(facts, { ...latest, ...change }));
  }
});
test("Docker latest readback ignores counters but rejects changed Release identity", () => {
  const latest = { id: 30, tag_name: "v2026.10.27", target_commitish: facts.source.revision, draft: false, prerelease: false, download_count: 1 };
  consumer.verifyLatest(latest, { ...latest, download_count: 2, body: "comment changed" });
  for (const change of [{ id: 31 }, { tag_name: "v2026.10.28" }, { target_commitish: "c".repeat(40) }]) assert.throws(() => consumer.verifyLatest(latest, { ...latest, ...change }));
});
test("Docker workflow keeps runtime assembly separate from the sole ARM producer", () => {
  const workflow = text(".github/workflows/docker-publish.yml"), docker = text("docker/Dockerfile"), compose = text("docker/docker-compose.yml");
  assert.match(workflow, /workflow_run:\n    workflows: \[Publish Crate\]\n    types: \[completed\]/);
  assert.doesNotMatch(workflow, /^  (?:push|pull_request):|frontend_package:|lotus_version:|\bnpm\b|\bcargo\b|context: \./m);
  assert.match(workflow, /needs: \[source, image\]/); assert.match(workflow, /fail-fast: false/);
  assert.match(workflow, /needs: \[source, arm-originals\]/);
  for (const platform of consumer.PLATFORMS) assert.equal(workflow.split("- platform: " + platform).length, 2);
  assert.ok(workflow.indexOf(".cjs smoke") < workflow.indexOf("uses: docker/login-action"));
  assert.ok(workflow.indexOf(".cjs smoke") < workflow.indexOf(".cjs push"));
  assert.match(workflow, /load: true\n          push: false/); assert.match(workflow, /retention-days: 90/);
  assert.doesNotMatch(workflow, /download-artifact|merge-multiple|pattern: docker-digest/);
  assert.match(workflow, /run: node scripts\/docker-from-release\.cjs select/);
  assert.match(workflow, /run: node scripts\/docker-from-release\.cjs reuse/);
  for (const name of ["Copy authenticated binary and Host originals", consumer.BUILD, consumer.SMOKE, consumer.PUSH, consumer.UPLOAD]) {
    assert.ok(workflow.includes("- name: " + name + "\n        if: steps.selection.outputs.mode == 'build'"), name);
  }
  assert.ok(workflow.indexOf(".cjs select") < workflow.indexOf(".cjs stage"));
  assert.match(workflow, /name: Verify retained original image digest\n        if: steps.selection.outputs.mode == 'reuse'/);
  assert.match(workflow, /image-ref: \$\{\{ needs.merge.outputs.image \}\}/);
  assert.match(workflow, /trivy-action@ed142fd0673e97e23eac54620cfb913e5ce36c25/); assert.match(workflow, /exit-code: "0"/);
  for (const [action, sha] of [["docker/setup-buildx-action", "f87e5991a6d7451dcb8d9637bfbc97413f497069"],
    ["docker/login-action", "81c6f93d4bc991e510b342a00b419b3bb4efc5c6"], ["docker/build-push-action", "c3c9e263c25d99ce0380d002d59b67737d91b0dc"],
    ["actions/upload-artifact", "cf430e030ddbb5b0abf93d22962f4752f3646cd9"]]) assert.ok(workflow.includes("uses: " + action + "@" + sha));
  assert.doesNotMatch(workflow, /uses: [^\n]+@v[0-9]/);
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

// Load only the existing thirty-crate setup; do not register or run its old suites.
async function source2361Fixture() {
  const Module = require("node:module"), filename = path.join(__dirname, "published-source-admission.test.cjs");
  const setup = fs.readFileSync(filename, "utf8"), terminal = "assert.equal((await f.verify()).crates.length,30);";
  assert.equal(setup.split(terminal).length, 2); let prepare;
  const m = new Module(filename, module); m.filename = filename; m.paths = Module._nodeModulePaths(__dirname);
  const realRequire = m.require.bind(m);
  m.require = name => name === "node:test" ? { test: (name, fn) => { if (name.startsWith("Main2351 public receipt accepts real thirty-crate")) prepare = fn; } } : realRequire(name);
  m._compile(setup.replace(terminal, "return f;"), filename); const f = await prepare();
  const realProof = require("./published-source-admission.cjs"), realSource = require("./frozen-source-admission.cjs");
  const D = "d".repeat(40), X = "e".repeat(40), T = second => at(10800 + second), events = [], sourceLogs = new Map();
  const run = { id: 8000, run_attempt: 1, workflow_id: 8001, check_suite_id: 8002, check_suite_node_id: "CS_docker2361",
    repository: f.state.run.repository, head_repository: f.state.run.head_repository, event: "workflow_dispatch", path: ".github/workflows/docker-publish.yml",
    name: "Docker", head_sha: X, head_branch: "main", status: "in_progress", created_at: T(0), updated_at: T(0) };
  const names = { source: consumer.SOURCE_JOB, "arm-originals": consumer.ARM_JOB, amd64: "Repackage originals (linux/amd64)", arm64: "Repackage originals (linux/arm64)", merge: "Publish complete multi-arch manifest" };
  const jobs = Object.entries(names).map(([key,name], i) => ({ id: 8100 + i, run_id: run.id, run_attempt: 1, name, head_sha: X, head_branch: "main",
    status: "in_progress", conclusion: null, started_at: T(i ? 30 : 1), completed_at: null, check_run_url: "https://api.github.com/check-runs/" + (8100 + i),
    steps: [{ number: 1, name: key === "source" ? consumer.SOURCE_STEP : "phase", status: "in_progress", conclusion: null, started_at: T(i ? 31 : 2), completed_at: null }] }));
  f.f.main.commit.sha = D; f.f.main.protection = { required_status_checks: { enforcement_level: "everyone", contexts: [realProof.MAIN_JOB], checks: [{ context: realProof.MAIN_JOB, app_id: 15368 }] } };
  const raw = f.reads, fixture = { f, D, X, run, jobs, events, sourceLogs, full: 0, hook: null, T, commands: [], artifacts: [], carriers: new Map(), outputs: [], writes: 0 };
  const reads = {};
  for (const key of ["json", "checkSuite", "jobLog", "assetBytes", "artifactBytes", "registryChecksum"]) reads[key] = async (...args) => {
    events.push({ key, args }); let value;
    if (key === "json") {
      const route = args[0];
      if (route === "") value = { id: 1164779771, full_name: "bigduu/Bamboo-agent", default_branch: "main" };
      else if (route === "actions/runs/8000") value = run;
      else if (route === "actions/workflows/8001") value = { id: 8001, path: run.path, state: "active" };
      else if (route.startsWith("actions/workflows/8001/runs?")) value = { total_count: 0, workflow_runs: [] };
      else if (route.startsWith("actions/runs/8000/artifacts?")) value = { total_count: fixture.artifacts.length, artifacts: fixture.artifacts };
      else if (route.startsWith("actions/artifacts/")) value=fixture.artifacts.find(row=>row.id===Number(route.slice(18)));
      else if (route==="releases/latest") value=fixture.latest;
      else if (route.startsWith("actions/runs/8000/jobs?")) value = { total_count: jobs.length, jobs };
      else if (route.startsWith("actions/runs/8000/attempts/")) { const rows = jobs.filter(job => job.run_attempt === Number(route.split("/")[4])); value = { total_count: rows.length, jobs: rows }; }
      else if (route.startsWith("actions/jobs/") && jobs.some(job => job.id === Number(route.slice(13)))) value = jobs.find(job => job.id === Number(route.slice(13)));
      else if (route.startsWith("releases/assets/")) value = f.state.assets.find(asset => asset.id === Number(route.slice(16)));
      else value = await raw.json(...args);
    } else if (key === "checkSuite" && args[0] === run.check_suite_node_id) value = suite(run);
    else if (key === "jobLog" && sourceLogs.has(args[0])) value = sourceLogs.get(args[0]);
    else if (key === "artifactBytes" && fixture.carriers.has(args[0])) value=fixture.carriers.get(args[0]);
    else value = await raw[key](...args);
    value = Buffer.isBuffer(value) ? Buffer.from(value) : structuredClone(value);
    return fixture.hook ? await fixture.hook(key, args, value) : value;
  };
  fixture.reads = reads;
  fixture.load = (role, platform = "linux/amd64", phase = role === "source" ? "source" : "select") => {
    const env = { GITHUB_REPOSITORY: "bigduu/Bamboo-agent", GITHUB_EVENT_NAME: run.event, GITHUB_REF: "refs/heads/main",
      GITHUB_WORKFLOW_REF: "bigduu/Bamboo-agent/.github/workflows/docker-publish.yml@refs/heads/main", GITHUB_WORKFLOW_SHA: D, GITHUB_SHA: D,
      GITHUB_EVENT_PATH: "source2361-event", GITHUB_JOB: role, GITHUB_RUN_ID: "8000", GITHUB_RUN_ATTEMPT: String(run.run_attempt), DOCKER_RELEASE_RUN: "900", DOCKER_PLATFORM: platform };
    if (fixture.stageRoot) Object.assign(env,{RUNNER_TEMP:fixture.stageRoot,GITHUB_OUTPUT:path.join(fixture.stageRoot,"outputs.txt")});
    const mod = { exports: {} }, fakeProcess = { env, argv: ["node", "fixture.cjs", phase] };
    const loader = name => {
      if (name === "./published-source-admission.cjs") return { ...realProof, ACCEPTED_RELEASE_PRODUCERS: f.policy.releaseProducers,
        verifyPublishedSource: async (input, ports) => { fixture.full++; return realProof.verifyPublishedSource(input, ports, f.policy); } };
      if (name === "./frozen-source-admission.cjs") return { ...realSource, ACCEPTED_PRODUCERS: f.policy.producers };
      if (name === "./main-admission.cjs") return { createPublicReads: () => reads };
      if (name === "node:fs") return { ...fs, readFileSync: (file,...args) => file === env.GITHUB_EVENT_PATH ? JSON.stringify({ inputs: { release_run: "900" } }) :
        fixture.workflowText && file===path.join(ROOT,".github/workflows/docker-publish.yml")?fixture.workflowText:fs.readFileSync(file,...args),
        appendFileSync:(file,data,...args)=>fixture.stageRoot?fs.appendFileSync(file,data,...args):fixture.outputs.push(String(data)) };
      if (name === "node:child_process") return { spawnSync: (program,args,options) => {
        fixture.commands.push(program);
        if (program==="git") return { status:0,stdout:args.at(-1)==="HEAD"?D+"\n":"" };
        if (fixture.stageRoot && program==="readelf") return {status:0,stdout:"Class: ELF64\nMachine: Advanced Micro Devices X86-64\n(NEEDED) [libc.so.6]\nGLIBC_2.36\n"};
        if (fixture.stageRoot && program==="python3") return spawnSync(program,args,options); // bounded Host archive parsing only
        if (fixture.digests && program==="python3") return spawnSync(program,args,options); // tiny retained digest ZIP only
        if (fixture.digests && program==="docker") {
          if(args.includes("create")){fixture.writes++;fixture.writtenTags=args.flatMap((arg,i)=>arg==="--tag"?[args[i+1]]:[]);return {status:0,stdout:""};}
          const value=args.at(-1).includes("@")?{digest:args.at(-1).split("@")[1]}:{digest:digest("3"),manifests:fixture.digests.map(row=>({digest:row.digest,platform:{os:"linux",architecture:row.platform.slice(6)}}))};
          return {status:0,stdout:JSON.stringify(value)}; // inert registry CLI port, never executes Docker
        }
        assert.fail("No product/compiler/registry command in Source fixtures: "+program);
      } };
      return require(name);
    };
    const fn = vm.runInThisContext("(function(require,module,exports,process,__dirname){" + text("scripts/docker-from-release.cjs").replace(/^#![^\n]*\n/, "") +
      "\nmodule.exports.__sourceState=authenticatedSource;module.exports.__stage=stage;module.exports.__main=main;})");
    fn(loader, mod, mod.exports, fakeProcess, __dirname); return mod.exports;
  };
  fixture.seal = async () => {
    const c = fixture.load("source"), state = await c.authenticated(), context = await c.consumerExecution(state, "source");
    fixture.sourceState = state; fixture.sourceContext = context; fixture.sourceModule = c;
    const line = c.sourceLine(state, run.id); sourceLogs.set(8100, T(10) + " " + line);
    jobs[0].status = "completed"; jobs[0].conclusion = "success"; jobs[0].completed_at = T(20);
    Object.assign(jobs[0].steps[0], { status: "completed", conclusion: "success", completed_at: T(19) });
    return c.parseSourceFrame(sourceLogs.get(8100));
  };
  fixture.prepareStage = () => {
    const sidecar=require("./sidecar-release.cjs"), files=host(), record=f.state.record;
    const replace=(ref,bytes)=>{Object.assign(ref,{sha256:core.sha256(bytes),sizeBytes:bytes.length});f.state.data.set(ref.assetId,bytes);
      Object.assign(f.state.assets.find(asset=>asset.id===ref.assetId),{size:bytes.length,digest:"sha256:"+ref.sha256});};
    const hostRef=record.native.assets.find(ref=>ref.name===sidecar.HOST_ASSET), manifestRef=record.native.assets.find(ref=>ref.name===sidecar.MANIFEST);
    replace(hostRef,tar(entries(files)));
    const value=JSON.parse(f.state.data.get(manifestRef.assetId));Object.assign(value.browserHost,{sha256:hostRef.sha256,sizeBytes:hostRef.sizeBytes,files:manifest(files).sort((a,b)=>a.path<b.path?-1:1)});
    replace(manifestRef,Buffer.from(JSON.stringify(value)));record.native.manifestSha256=manifestRef.sha256;
    fixture.stageRoot=fs.mkdtempSync(path.join(require("node:os").tmpdir(),"bamboo2361-stage-"));
    jobs[2].steps=[{number:1,name:consumer.SELECT,status:"completed",conclusion:"success",started_at:T(31),completed_at:T(32)},
      {number:2,name:"Copy authenticated binary and Host originals",status:"in_progress",conclusion:null,started_at:T(33),completed_at:null}];
    return hostRef.assetId;
  };
  fixture.prepareDigests = () => {
    fixture.digests=consumer.PLATFORMS.map((platform,i)=>({...consumer.selected(fixture.sourceState.facts),platform,digest:digest(String(i+1))}));
    fixture.latest={id:f.state.release.id,tag_name:f.state.release.tag_name,target_commitish:f.state.release.target_commitish,draft:false,prerelease:false};
    fixture.digests.forEach((record,i)=>{
      const current=imageJob(8102+i,1);Object.assign(current,{run_id:run.id,head_sha:X,name:"Repackage originals ("+record.platform+")",started_at:T(30),completed_at:T(50)});
      current.steps.forEach((step,n)=>Object.assign(step,{started_at:T(31+n),completed_at:T(32+n)}));Object.assign(jobs[2+i],current);
      const raw=zipDigest(consumer.digestFile(run.id,1,record.platform),record),artifact={id:9000+i,name:"docker-digest-8000-1-"+i,expired:false,size_in_bytes:raw.length,digest:"sha256:"+core.sha256(raw),created_at:T(37),workflow_run:{id:run.id,head_sha:X,head_branch:"main"}};
      fixture.artifacts.push(artifact);fixture.carriers.set(artifact.id,raw);
      sourceLogs.set(current.id,T(37)+` Artifact ${artifact.name} has been successfully uploaded! Final size is ${raw.length} bytes. Artifact ID is ${artifact.id}\n`+
        T(37)+` Artifact download URL: https://github.com/bigduu/Bamboo-agent/actions/runs/${run.id}/artifacts/${artifact.id}\n`);
    });
    jobs[4].steps[0].name="Recheck source and publish both architectures together";
  };
  return fixture;
}
test("Docker Source2361 real thirty-crate proof fits a bounded frame and runs full verification once", async () => {
  const f = await source2361Fixture(), sealed = await f.seal();
  assert.equal(sealed.data.facts.crates.length, 30); assert.equal(sealed.data.featurePhysical.length, 12); assert.equal(sealed.data.releasePhysical.length, 7);
  const size = Buffer.byteLength(core.canonicalJson(sealed)); assert.ok(size < 65536); assert.ok(size > 16000);
  const before = f.events.length, c = f.load("image"), state = await c.authenticated(); await c.consumerExecution(state, "image");
  assert.equal(f.full, 1); assert.equal(state.controller, f.D); assert.notEqual(f.X, f.D);
  assert.ok(!f.events.slice(before).some(event => event.key === "assetBytes" || event.key === "json" && /^actions\/jobs\/(?:10[0-9]|11[01]|200[0-6])$/.test(event.args[0])));
  console.log("Source2361 frame bytes:", size);
});
test("Docker Source2361 rejects changed whole completion after the first Source", async () => {
  const f = await source2361Fixture(); await f.seal();
  f.f.state.record.source.tree = "1".repeat(40);
  await assert.rejects(f.load("image").authenticated(), /completion/i);
});
test("Docker Source2361 all fixed-role authentication and guard budget", async () => {
  const f = await source2361Fixture(), groups = {};
  const count = rows => ({ rest: rows.filter(row => ["json", "jobLog", "assetBytes"].includes(row.key)).length,
    graphql: rows.filter(row => row.key === "checkSuite").length,
    crates: rows.filter(row => row.key === "registryChecksum").length });
  let start = 0; await f.seal(); await f.sourceModule.crossRunGuard(f.sourceState, f.sourceContext);
  groups.source = count(f.events); start = f.events.length;
  for (const [label,role,n,refresh,platform] of [["arm","arm-originals",5,[2,4]], ["amd64","image",4,[3],"linux/amd64"],
    ["arm64","image",4,[3],"linux/arm64"], ["merge","merge",1,[0]]]) {
    for (let i=0;i<n;i++) {
      const c=f.load(role,platform), staging=role==="image" && i===1;
      const state=await (staging ? c.__sourceState() : c.authenticated()), context=await c.consumerExecution(state,role);
      if (role!=="image" || i===0 || i===3) await c.crossRunGuard(state,context);
      if (role==="arm-originals" && i===4) await c.crossRunGuard(state,context); // seal's original-output reader
      if (staging) { await c.refreshPublication(state); await c.freshConsumer(context,"phase"); }
      if (refresh.includes(i)) { await c.refreshPublication(state); await c.freshConsumer(context,"phase"); }
    }
    groups[label]=count(f.events.slice(start)); start=f.events.length;
  }
  const measured=count(f.events), unchangedConsumerReads=56; // retained asset/carrier/latest paths from the frozen route audit
  assert.equal(f.full,1); assert.equal(measured.crates,600);
  console.log("Source2361 actual role budget:",JSON.stringify({groups,measured,unchangedConsumerReads,bound:measured.rest+unchangedConsumerReads}));
  assert.ok(measured.rest+unchangedConsumerReads<=850,"All fixed-role reads plus unchanged consumer paths exceed850");
});
for (const [name,mutate] of [
  ["default branch",f=>{f.hook=(key,args,value)=>key==="json"&&args[0]===""?{...value,default_branch:"dev"}:value;}],
  ["Main protection",f=>{f.f.f.main.protection.required_status_checks.checks[0].app_id=1;}],
  ["Dev protection",f=>{f.f.f.dev.protection.required_status_checks.enforcement_level="non_admins";}],
  ["frozen ref",f=>{f.f.f.release.commit.sha="9".repeat(40);}],
  ["feature PR",f=>{f.f.f.pr.merged=false;}],
  ["newer feature run",f=>{f.f.f.newerRun=true;}],
  ["sealed feature attempt",f=>{f.f.f.run.run_attempt++;}],
  ["latest feature failure",f=>{f.f.f.latest[1].conclusion="failure";}],
  ["original feature projection",f=>{f.f.f.attempts[1][0].steps[0].conclusion="skipped";}],
  ["publisher failure",f=>{f.f.state.run.conclusion="failure";}],
  ["original publisher projection",f=>{f.f.state.physical[0].steps[0].name="changed";}],
  ["Release state",f=>{f.f.state.release.draft=true;}],
  ["tag source",f=>{f.f.state.tag.object.sha="9".repeat(40);}],
  ["asset identity",f=>{f.f.state.assets[0].id=99999;}],
  ["registry checksum",f=>{f.f.state.registry.set(f.f.state.record.crates[0].name,"0".repeat(64));}],
]) test("Docker Source2361 fresh guard rejects "+name,async()=>{
  const f=await source2361Fixture();await f.seal();mutate(f);
  await assert.rejects(f.load("image").authenticated());assert.equal(f.full,1);
});
test("Docker Source2361 anchor equality is reflexive and normal advancement still needs official ancestry",async()=>{
  const f=await source2361Fixture();await f.seal();let comparisons=0;
  f.hook=(key,args,value)=>{if(key==="json"&&args[0].startsWith("compare/")){comparisons++;throw Error("No reflexive compare transport required");}return value;};
  await f.load("image").authenticated();assert.equal(comparisons,0);
  f.hook=null;f.f.f.main.commit.sha="8".repeat(40);f.f.f.dev.commit.sha="7".repeat(40);
  // Closed-PR cached base.sha and counters are not accepted source identities.
  f.f.f.pr.base.sha="6".repeat(40);f.f.state.release.download_count=999;
  await f.load("image").authenticated();
  assert.ok(f.events.some(row=>row.key==="json"&&row.args[0]===`compare/${f.D}...${"8".repeat(40)}`));
  assert.ok(f.events.some(row=>row.key==="json"&&row.args[0]===`compare/${f.f.input.sourceRevision}...${"7".repeat(40)}`));
});
for(const branch of ["main","dev"])test("Docker Source2361 anchor rejects "+branch+" rewind or foreign history",async()=>{
  const f=await source2361Fixture();await f.seal();f.f.f[branch].commit.sha="8".repeat(40);
  f.hook=(key,args,value)=>key==="json"&&args[0].startsWith("compare/")?{...value,status:"diverged"}:value;
  await assert.rejects(f.load("image").authenticated(),/ancestry/);
});
test("Docker Source2361 direct-only immutable checks stay mandatory for first Source and are not repeated downstream",async()=>{
  for(const variant of ["apiFailure","differentPhysicalId"]){
    const first=await source2361Fixture();if(variant==="apiFailure")first.f.f.apiFailure=true;else first.f.f.physical[0].id=999;
    await assert.rejects(first.seal());assert.equal(first.full,1);
  }
  const f=await source2361Fixture();await f.seal();const start=f.events.length;
  f.f.f.apiFailure=true;f.f.f.physical[0].id=999;
  await f.load("image").authenticated();assert.equal(f.full,1);
  assert.ok(!f.events.slice(start).some(row=>row.key==="json"&&row.args[0]==="actions/jobs/100"));
});
function source2361NextAttempt(f,copied=false){
  const attempt=++f.run.run_attempt;
  const copies=f.jobs.filter(job=>job.run_attempt===1).map(job=>({...structuredClone(job),id:job.id+100*(attempt-1),run_attempt:attempt}));
  if(!copied)Object.assign(copies[0],{status:"in_progress",conclusion:null,started_at:f.T(40+attempt),completed_at:null,
    steps:[{name:consumer.SOURCE_STEP,number:1,status:"in_progress",conclusion:null,started_at:f.T(41+attempt),completed_at:null}]});
  f.jobs.push(...copies);return copies[0];
}
test("Docker Source2361 copied Source and publisher successes preserve physical originals",async()=>{
  const f=await source2361Fixture();await f.seal();source2361NextAttempt(f,true);
  const state=f.f.state;state.run.run_attempt=2;state.suite.workflowRun.runAttempt=2;
  const copies=state.physical.map(job=>({...structuredClone(job),id:job.id+100,run_attempt:2}));state.physical.push(...copies);state.latest=structuredClone(copies);
  const c=f.load("image"),held=await c.authenticated();await c.consumerExecution(held,"image");
  assert.equal(f.full,1);assert.equal(held.facts.controller.finalizerJobId,2006);
  assert.ok(f.events.some(row=>row.key==="json"&&row.args[0]==="actions/jobs/8100"));
  assert.ok(!f.events.some(row=>row.key==="json"&&row.args[0]==="actions/jobs/8200"));
});
test("Docker Source2361 full rerun emits guarded reuse linked to the first real full proof",async()=>{
  const f=await source2361Fixture();await f.seal();const second=source2361NextAttempt(f);
  const c=f.load("source"),state=await c.authenticated();await c.consumerExecution(state,"source");
  const line=f.T(45)+" "+c.sourceLine(state,f.run.id),record=c.parseSourceFrame(line);
  assert.equal(record.mode,"reuse");assert.equal(record.data,null);assert.deepEqual(record.origin,{id:8100,attempt:1});
  f.sourceLogs.set(second.id,line);Object.assign(second,{status:"completed",conclusion:"success",completed_at:f.T(50)});
  Object.assign(second.steps[0],{status:"completed",conclusion:"success",completed_at:f.T(49)});
  await f.load("image").authenticated();assert.equal(f.full,1);
  f.sourceLogs.delete(8100);await assert.rejects(f.load("image").authenticated());assert.equal(f.full,1);
});
for(const status of ["failure","cancelled","skipped","in_progress"])test("Docker Source2361 latest Source "+status+" blocks older green",async()=>{
  const f=await source2361Fixture();await f.seal();const latest=source2361NextAttempt(f);
  Object.assign(latest,{status:status==="in_progress"?status:"completed",conclusion:status==="in_progress"?null:status});
  await assert.rejects(f.load("image").authenticated());assert.equal(f.full,1);
});
test("Docker Source2361 workers have no full-proof fallback before Source success",async()=>{
  const f=await source2361Fixture();await assert.rejects(f.load("image").authenticated());assert.equal(f.full,0);
});
test("Docker Source2361 official consumer file metadata binds X instead of claiming executed D",async()=>{
  const f=await source2361Fixture();await f.seal();
  f.hook=(key,args,value)=>{if(key==="checkSuite"&&args[0]===f.run.check_suite_node_id)value.workflowRun.file.repositoryFileUrl=value.workflowRun.file.repositoryFileUrl.replace(f.X,f.D);return value;};
  await assert.rejects(f.load("image").authenticated());assert.equal(f.full,1);
});
function source2361NoOutputs(f){
  f.jobs.push({...structuredClone(f.jobs[4]),id:8105,name:"Scan exact published image (Trivy)"});
  Object.assign(f.jobs[0],{status:"completed",conclusion:"failure",completed_at:f.T(20)});
  Object.assign(f.jobs[0].steps[0],{status:"completed",conclusion:"failure",completed_at:f.T(19)});
  for(const job of f.jobs.slice(1))Object.assign(job,{status:"completed",conclusion:"skipped",steps:[]});
}
test("Docker Source2361 unread Source retry recovers a failed first API read before any outputs",async()=>{
  const f=await source2361Fixture();f.f.f.apiFailure=true;await assert.rejects(f.seal(),/Original API evidence unavailable/);
  assert.equal(f.full,1);f.f.f.apiFailure=false;source2361NoOutputs(f);const second=source2361NextAttempt(f);
  const c=f.load("source"),state=await c.authenticated();await c.consumerExecution(state,"source");
  const line=f.T(45)+" "+c.sourceLine(state,f.run.id),record=c.parseSourceFrame(line);
  assert.equal(f.full,2,"First read failed; only the second full proof completed");assert.equal(record.mode,"full");assert.deepEqual(record.origin,{id:8200,attempt:2});
  f.sourceLogs.set(second.id,line);Object.assign(second,{status:"completed",conclusion:"success",completed_at:f.T(50)});
  Object.assign(second.steps[0],{status:"completed",conclusion:"success",completed_at:f.T(49)});
  const image=f.jobs.find(job=>job.id===8202);Object.assign(image,{status:"in_progress",conclusion:null,steps:[{name:"phase",number:1,status:"in_progress"}]});
  await f.load("image").authenticated();assert.equal(f.full,2);
});
for(const variant of ["worker-success","worker-failure","worker-running","missing-worker","unknown-worker","nonempty-steps","matrix-placeholder","missing-attempt","pending-Source"])
test("Docker Source2361 unread Source retry stops on "+variant,async()=>{
  const f=await source2361Fixture();source2361NoOutputs(f);source2361NextAttempt(f);
  if(variant==="worker-success")f.jobs[1].conclusion="success";
  if(variant==="worker-failure")f.jobs[1].conclusion="failure";
  if(variant==="worker-running")f.jobs[1].status="in_progress";
  if(variant==="missing-worker")f.jobs.splice(1,1);
  if(variant==="unknown-worker")f.jobs[1].name="unknown";
  if(variant==="nonempty-steps")f.jobs[1].steps=[{status:"completed",conclusion:"skipped"}];
  if(variant==="matrix-placeholder")f.jobs[2].name="Repackage originals";
  if(variant==="pending-Source")f.jobs[0].status="in_progress";
  if(variant==="missing-attempt")f.hook=(key,args,value)=>key==="json"&&args[0].startsWith("actions/runs/8000/attempts/1/jobs")?{...value,total_count:0,jobs:[]}:value;
  await assert.rejects(f.load("source").authenticated());assert.equal(f.full,0);
});
test("Docker Source2361 unread Source retry never replaces a recorded successful proof",async()=>{
  const f=await source2361Fixture();await f.seal();source2361NextAttempt(f);f.sourceLogs.delete(8100);
  await assert.rejects(f.load("source").authenticated());assert.equal(f.full,1);
});
function source2361MatrixPlaceholder(f){
  f.jobs[2].name="Repackage originals (${{ matrix.platform }})";f.jobs.splice(3,1);
}
test("Docker Source2361 exact bound-D matrix placeholder retries only the failed first read",async()=>{
  const f=await source2361Fixture();f.f.f.apiFailure=true;await assert.rejects(f.seal());
  f.f.f.apiFailure=false;source2361NoOutputs(f);source2361MatrixPlaceholder(f);source2361NextAttempt(f);
  const c=f.load("source"),state=await c.authenticated(),record=c.parseSourceFrame(f.T(45)+" "+c.sourceLine(state,f.run.id));
  assert.equal(record.mode,"full");assert.deepEqual(record.origin,{id:8200,attempt:2});assert.equal(f.full,2);
});
for(const variant of ["unknown-template","mixed","duplicate","non-skip","executed-step","changed-D-matrix","incomplete-attempt"])
test("Docker Source2361 bound matrix placeholder rejects "+variant,async()=>{
  const f=await source2361Fixture();source2361NoOutputs(f);source2361MatrixPlaceholder(f);
  if(variant==="unknown-template")f.jobs[2].name="Repackage originals (${{ matrix.arch }})";
  if(variant==="mixed")f.jobs.push({...structuredClone(f.jobs[2]),id:8190,name:"Repackage originals (linux/amd64)"});
  if(variant==="duplicate")f.jobs.push({...structuredClone(f.jobs[2]),id:8191});
  if(variant==="non-skip")f.jobs[2].conclusion="failure";
  if(variant==="executed-step")f.jobs[2].steps=[{status:"completed",conclusion:"success"}];
  if(variant==="changed-D-matrix")f.workflowText=text(".github/workflows/docker-publish.yml").replace("- platform: linux/arm64","- platform: linux/arm/v7");
  source2361NextAttempt(f);
  if(variant==="incomplete-attempt")f.hook=(key,args,value)=>key==="json"&&args[0].startsWith("actions/runs/8000/attempts/1/jobs")?{...value,total_count:0,jobs:[]}:value;
  await assert.rejects(f.load("source").authenticated());assert.equal(f.full,0);
});
for(const variant of ["direct","lost-log","duplicate-log","outside-window","foreign-D","foreign-R","JSON-stub"])
test("Docker Source2361 Source origin rejects "+variant,async()=>{
  const f=await source2361Fixture();await f.seal();let line=f.sourceLogs.get(8100);
  if(variant==="direct")f.hook=(key,args,value)=>{if(key==="json"&&args[0]==="actions/jobs/8100")throw Error("Source direct unavailable");return value;};
  if(variant==="lost-log")f.sourceLogs.delete(8100);
  if(variant==="duplicate-log")f.sourceLogs.set(8100,line+"\n"+line);
  if(variant==="outside-window")f.sourceLogs.set(8100,line.replace(f.T(10),f.T(25)));
  if(["foreign-D","foreign-R","JSON-stub"].includes(variant)){
    const value=f.sourceModule.parseSourceFrame(line);
    if(variant==="foreign-D")value.selection.controller="f".repeat(40);
    if(variant==="foreign-R")value.selection.consumerRun++;
    if(variant==="JSON-stub")value.data={publicationAuthorized:false,success:true};
    f.sourceLogs.set(8100,f.T(10)+" "+consumer.SOURCE_PREFIX+Buffer.from(core.canonicalJson(value)).toString("base64"));
  }
  await assert.rejects(f.load("image").authenticated());assert.equal(f.full,1);
});
test("Docker Source2361 stage validates final mutable authority before exposing a build context",async()=>{
  const f=await source2361Fixture();f.prepareStage();await f.seal();await f.load("image").__stage();
  const output=fs.readFileSync(path.join(f.stageRoot,"outputs.txt"),"utf8");assert.match(output,/^context=/m);assert.match(output,/^image=/m);
  assert.equal(f.full,1);assert.ok(f.commands.every(program=>["git","readelf","python3"].includes(program)));
});
for(const variant of ["ref","feature","Release","Source-log","consumer-attempt"])
test("Docker Source2361 stage drift during byte read blocks context and build: "+variant,async()=>{
  const f=await source2361Fixture(),hostId=f.prepareStage();await f.seal();let changed=false;
  f.hook=(key,args,value)=>{if(!changed&&key==="assetBytes"&&args[0]===hostId){changed=true;
    if(variant==="ref")f.f.f.release.commit.sha="9".repeat(40);
    if(variant==="feature")f.f.f.latest[1].conclusion="failure";
    if(variant==="Release")f.f.state.release.draft=true;
    if(variant==="Source-log")f.sourceLogs.delete(8100);
    if(variant==="consumer-attempt")f.run.run_attempt++;
  }return value;};
  await assert.rejects(f.load("image").__stage());assert.equal(changed,true);assert.equal(f.full,1);
  assert.equal(fs.existsSync(path.join(f.stageRoot,"outputs.txt")),false,"No context output can enable an image build");
  assert.ok(fs.existsSync(path.join(f.stageRoot,"bamboo-docker-context","originals.json")),"Retain owned scratch for diagnosis");
  assert.ok(!f.commands.some(program=>["docker","cargo","npm"].includes(program)));
});
