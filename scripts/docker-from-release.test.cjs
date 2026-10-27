"use strict";
// Data/transport/static-routing fixtures only: no Docker, Cargo or browser run.
const assert = require("node:assert/strict"), test = require("node:test");
const fs = require("node:fs"), path = require("node:path");
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
  const fixture = { run, physical, raw, artifact, jobs: [physical], artifacts: [artifact], getCalls: [], byteCalls: 0 };
  const get = async route => { fixture.getCalls.push(route); if (route === "actions/jobs/101") return fixture.physical; if (route === "actions/artifacts/900") return fixture.artifact; throw Error("unexpected read " + route); };
  fixture.context = { run, get, paged: async () => fixture.artifacts };
  fixture.state = { controller: "d".repeat(40), facts, input: { releaseRun: 10 }, reads: { artifactBytes: async (id, maximum) => { assert.equal(id, 900); assert.equal(maximum, fixture.artifact.size_in_bytes); fixture.byteCalls++; return fixture.raw; } } };
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
  assert.deepEqual(f.getCalls, ["actions/jobs/101", "actions/artifacts/900", "actions/jobs/101"]);
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
  await assert.rejects(consumer.consumerExecution(f.state,"merge"), /Actual job key/);
  f.state.reads.checkSuite = async () => ({ ...suite(f.run), app: { databaseId: 1, slug: "foreign" } }); await assert.rejects(consumer.consumerExecution(f.state,"image"));
  f.state.reads.checkSuite = async () => suite(f.run); const previousRead = f.state.reads.json;
  f.state.reads.json = async route => route.includes("/jobs?") ? { jobs: [f.physical,current] } : previousRead(route); await assert.rejects(consumer.consumerExecution(f.state,"image"), /total_count/);
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
test("Docker workflow has one runtime consumer, no source or frontend rebuild", () => {
  const workflow = text(".github/workflows/docker-publish.yml"), docker = text("docker/Dockerfile"), compose = text("docker/docker-compose.yml");
  assert.match(workflow, /workflow_run:\n    workflows: \[Publish Crate\]\n    types: \[completed\]/);
  assert.doesNotMatch(workflow, /^  (?:push|pull_request):|frontend_package:|lotus_version:|\bnpm\b|\bcargo\b|context: \./m);
  assert.match(workflow, /needs: \[source, image\]/); assert.match(workflow, /fail-fast: false/);
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
