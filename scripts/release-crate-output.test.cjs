"use strict";
const assert = require("node:assert/strict");
const { test } = require("node:test");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const crypto = require("node:crypto");
const { spawnSync } = require("node:child_process");
const output = require("./release-crate-output.cjs");
const uploader = require("./release-cargo-upload.cjs");
const sandbox = require("./release-cargo-sandbox.cjs");
const C = "a".repeat(40), S = "b".repeat(40), TREE = "c".repeat(40);
const hash = bytes => crypto.createHash("sha256").update(bytes).digest("hex");
const clone = value => JSON.parse(JSON.stringify(value));
const childEnv = () => Object.fromEntries(["PATH", "HOME"].filter(key => process.env[key]).map(key => [key, process.env[key]]));
function python(code, input, args = []) {
  const result = spawnSync("python3", ["-I", "-c", code, ...args], { input, env: childEnv(), maxBuffer: 16 * 1024 ** 2 });
  assert.equal(result.status, 0, result.stderr.toString()); return result.stdout;
}
function expected(name = "fixture-output") {
  return { repository: output.REPOSITORY, releaseId: 7, sourceRevision: S, sourceTree: TREE, version: "2026.10.27", name,
    pathInVcs: `crates/${name}`, frontendBytes: { manifestSha256: hash(Buffer.from("manifest")), archiveSha256: hash(Buffer.from("frontend")) } };
}
function archive(value, changes = {}) {
  const entries = [
    ["Cargo.toml", `[package]\nname="${value.name}"\nversion="${value.version}"\nedition="2021"\ndescription="Fixture"\nlicense="MIT"\n`],
    ["src/lib.rs", changes.sourceText || "pub fn fixture() {}\n"],
    [".cargo_vcs_info.json", JSON.stringify({ git: { sha1: changes.source || value.sourceRevision, dirty: changes.dirty ?? true }, path_in_vcs: changes.layout ?? value.pathInVcs })],
  ];
  if (value.name === "bamboo-server") entries.push(["frontend_package/frontend-manifest.json", changes.frontend || "manifest"], ["frontend_package/lotus-frontend.zip", "frontend"]);
  if (changes.extra) entries.push(changes.extra);
  return python(String.raw`
import gzip,io,json,sys,tarfile
prefix=sys.argv[1]+'/'; entries=json.load(sys.stdin); raw=io.BytesIO()
with tarfile.open(fileobj=raw,mode='w',format=tarfile.USTAR_FORMAT) as tar:
    for name,text in entries:
        data=text.encode(); info=tarfile.TarInfo(prefix+name); info.size=len(data); info.mtime=0
        tar.addfile(info,io.BytesIO(data))
sys.stdout.buffer.write(gzip.compress(raw.getvalue(),mtime=0))
`, JSON.stringify(entries), [`${value.name}-${value.version}`]);
}
const archiveCache = new Map();
function fixtureBytes(value) {
  const key = JSON.stringify(value); if (!archiveCache.has(key)) archiveCache.set(key, archive(value));
  return Buffer.from(archiveCache.get(key));
}
function world(value = expected()) {
  const bytes = fixtureBytes(value), assets = new Map(), counts = { prepare: 0, assetUpload: 0, registryUpload: 0, guard: 0, save: 0, wait: [] }, events = [], routes = [];
  const run = { id: 41, run_attempt: 1, repository: { id: 1164779771, full_name: output.REPOSITORY }, head_repository: { id: 1164779771, full_name: output.REPOSITORY },
    event: "workflow_dispatch", path: output.WORKFLOW, head_branch: "main", head_sha: C, status: "in_progress", conclusion: null };
  const job = { id: 501, run_id: 41, run_attempt: 1, name: output.JOB, head_branch: "main", head_sha: C,
    status: "in_progress", conclusion: null, started_at: "2026-10-27T01:00:00Z", completed_at: null };
  const environment = { GITHUB_REPOSITORY: output.REPOSITORY, GITHUB_EVENT_NAME: "workflow_dispatch", GITHUB_REF: "refs/heads/main", GITHUB_SHA: C,
    GITHUB_WORKFLOW_SHA: C, GITHUB_WORKFLOW_REF: `${output.REPOSITORY}/${output.WORKFLOW}@refs/heads/main`, GITHUB_JOB: "publish-crates", GITHUB_RUN_ID: "41", GITHUB_RUN_ATTEMPT: "1" };
  const w = { expected: value, bytes, assets, counts, events, routes, environment, run, job, state: null, published: null, outcomes: ["ok"], requests: [], readHook: null, prepareError: null, assetError: null, saveError: null };
  w.ports = {
    environment,
    load: async () => w.state === null ? null : clone(w.state),
    save: async (next, previous) => {
      assert.deepEqual(previous, w.state, "One authenticated reservation writer/CAS");
      if (w.saveError?.(next)) throw new Error("Fixture reservation write interrupted");
      counts.save++; events.push("save:" + next.phase + (next.sealed ? ":sealed" : "")); w.state = clone(next);
    },
    read: async route => {
      routes.push(route); if (w.readHook) { const result = w.readHook(route); if (result !== undefined) return result; }
      if (route === "actions/runs/41") return clone(run);
      if (route === "actions/runs/41/jobs?filter=all&per_page=100&page=1") return { total_count: 1, jobs: [clone(job)] };
      if (route === "actions/jobs/501") return clone(job);
      if (route === "releases/7") return { id: 7, tag_name: `v${value.version}`, target_commitish: S };
      if (route === "releases/7/assets?per_page=100&page=1") return [...assets.values()].map(item => clone(item.metadata));
      const match = route.match(/^releases\/assets\/(\d+)$/);
      if (match) return clone(assets.get(Number(match[1])).metadata);
      throw new Error("Unexpected raw API route: " + route);
    },
    readAsset: async id => Buffer.from(assets.get(id).bytes),
    uploadAsset: async (name, zip) => {
      assert.equal(w.state.phase, "preparing"); assert.equal(w.state.sealed.sha256, hash(zip));
      assert.ok(![...assets.values()].some(item => item.metadata.name === name), "No replacement uploads");
      counts.assetUpload++; events.push("asset:upload");
      if (w.assetError === "before") throw new Error("Fixture asset write failed");
      assets.set(1001, { metadata: { id: 1001, name, size: zip.length, digest: `sha256:${hash(zip)}`, state: "uploaded", created_at: "2026-10-27T01:01:00Z" }, bytes: Buffer.from(zip) });
      if (w.assetError === "after") throw new Error("Fixture response lost after upload");
    },
    prepare: async item => {
      assert.equal(w.state.phase, "preparing"); assert.equal(w.state.sealed, undefined); assert.deepEqual(item, value);
      counts.prepare++; events.push("prepare"); if (w.prepareError) throw new Error(w.prepareError);
      return { bytes: Buffer.from(bytes), verification: { preparationCommand: ["package", "--locked", "--allow-dirty", "--no-verify", "-p", value.name], command: ["package", "--locked", "--allow-dirty", "-p", value.name], image: sandbox.IMAGE, exitCode: 0 } };
    },
    registry: async () => w.published === null ? null : { checksum: hash(w.published) },
    registryArchive: async () => Buffer.from(w.published),
    guard: async () => { counts.guard++; events.push("source:guard"); },
    upload: async (actual, metadata) => uploader.uploadArchive(actual, metadata, "dummy-fixture-token", { fetchImpl: async (url, options) => {
      assert.equal(url, "https://crates.io/api/v1/crates/new"); assert.equal(options.method, "PUT"); assert.equal(options.redirect, "error");
      const body = Buffer.from(options.body), length = body.readUInt32LE(0), description = JSON.parse(body.subarray(4, 4 + length));
      assert.deepEqual(description, uploader.describeArchive(bytes, value.name, value.version));
      assert.equal(body.readUInt32LE(4 + length), bytes.length); assert.deepEqual(body.subarray(8 + length), bytes);
      counts.registryUpload++; events.push("registry:upload"); w.requests.push(body);
      const outcome = w.outcomes.shift() || "ok";
      if (outcome === "429") return new Response("{}", { status: 429, headers: { "retry-after": "0" } });
      if (outcome === "unknown") throw new Error("Fixture transport failure");
      w.published = Buffer.from(actual);
      if (outcome === "lost") throw new Error("Fixture response lost after registry accepted");
      return new Response("{}", { status: 200 });
    } }),
    wait: async seconds => { counts.wait.push(seconds); },
  };
  w.finish = conclusion => { job.status = run.status = "completed"; job.conclusion = run.conclusion = conclusion; job.completed_at = "2026-10-27T01:10:00Z"; };
  return w;
}
async function verified(w) {
  w.outcomes = ["unknown"];
  await assert.rejects(output.publishCrate(w.expected, w.ports), /unconfirmed/);
  assert.equal(w.state.phase, "verified"); return w;
}

test("one verified original is durably read back before the official raw archive upload", async () => {
  const w = world(); const state = await output.publishCrate(w.expected, w.ports);
  assert.equal(state.phase, "published"); assert.equal(w.counts.prepare, 1); assert.equal(w.counts.assetUpload, 1); assert.equal(w.counts.registryUpload, 1);
  assert.deepEqual(w.events, ["save:preparing", "prepare", "save:preparing:sealed", "asset:upload", "save:verified:sealed", "source:guard", "registry:upload", "save:published:sealed"]);
  assert.equal(w.counts.guard, 1);
  assert.equal(state.origin.jobId, 501); assert.equal(state.origin.jobKey, "publish-crates");
  assert.notEqual(String(state.origin.jobId), state.origin.jobKey);
  assert.ok(w.routes.includes("actions/jobs/501"));
  w.finish("success"); const closure = await output.readVerifiedClosure([w.expected], [state], w.ports);
  assert.equal(closure.publicationAuthorized, false); assert.equal(closure.crates[0].originalJob.conclusion, "success");
  assert.equal(closure.crates[0].archive.sha256, hash(w.bytes));
});

test("429 retries send byte-identical official requests from one prepared/verified original", async () => {
  const w = world(); w.outcomes = ["429", "429", "ok"];
  await output.publishCrate(w.expected, w.ports);
  assert.equal(w.counts.prepare, 1); assert.equal(w.counts.assetUpload, 1); assert.equal(w.counts.registryUpload, 3);
  assert.equal(w.counts.guard, 3);
  assert.ok(w.counts.wait.every(seconds => seconds >= 15 && seconds <= 1200));
  assert.ok(w.requests.every(bytes => bytes.equals(w.requests[0])));
});

test("unknown upload retains verified bytes; a new invocation restores without another package", async () => {
  const w = await verified(world()); const state = clone(w.state);
  await output.publishCrate(w.expected, w.ports);
  assert.equal(w.state.phase, "published"); assert.equal(w.counts.prepare, 1); assert.equal(w.counts.assetUpload, 1);
  assert.deepEqual(w.state.origin, state.origin); assert.deepEqual(w.state.sealed, state.sealed); assert.deepEqual(w.state.asset, state.asset);
  const counts = { ...w.counts }; await output.publishCrate(w.expected, w.ports);
  assert.equal(w.counts.save, counts.save); assert.equal(w.counts.registryUpload, counts.registryUpload);
});

test("lost upload response is confirmed by original registry bytes, not by repackaging", async () => {
  const w = world(); w.outcomes = ["lost"]; await output.publishCrate(w.expected, w.ports);
  assert.equal(w.state.phase, "published"); assert.equal(w.counts.prepare, 1); assert.equal(w.counts.registryUpload, 1);
});

test("unknown prior preparation cannot be retried or replaced by an unclaimed ZIP", async () => {
  const w = world(); w.prepareError = "Fixture interrupted verification";
  await assert.rejects(output.publishCrate(w.expected, w.ports), /interrupted verification/);
  assert.equal(w.state.phase, "preparing"); assert.equal(w.state.sealed, undefined);
  w.prepareError = null; await assert.rejects(output.publishCrate(w.expected, w.ports), /unknown/);
  assert.equal(w.counts.prepare, 1); assert.equal(w.counts.registryUpload, 0);
});

test("successful verification with missing retained ZIP stops explicit recovery", async () => {
  const w = world(); w.assetError = "before";
  await assert.rejects(output.publishCrate(w.expected, w.ports), /ZIP missing/);
  assert.ok(w.state.sealed); w.assetError = null;
  await assert.rejects(output.publishCrate(w.expected, w.ports), /ZIP missing/);
  assert.equal(w.counts.prepare, 1); assert.equal(w.counts.assetUpload, 1); assert.equal(w.counts.registryUpload, 0);
});

test("lost asset response and interrupted verified save recover by the pre-upload authenticated ZIP hash", async () => {
  const w = world(); w.assetError = "after"; w.saveError = next => next.phase === "verified";
  await assert.rejects(output.publishCrate(w.expected, w.ports), /checkpoint save is unconfirmed/);
  assert.equal(w.state.phase, "preparing"); assert.ok(w.state.sealed); assert.equal(w.assets.size, 1);
  w.saveError = null; await output.publishCrate(w.expected, w.ports);
  assert.equal(w.state.phase, "published"); assert.equal(w.counts.prepare, 1); assert.equal(w.counts.assetUpload, 1);
});

test("a new v2 crate cannot adopt an existing registry version or orphaned Release asset", async () => {
  const w = world(); w.published = Buffer.from(w.bytes);
  await assert.rejects(output.publishCrate(w.expected, w.ports), /no verified original checkpoint/); assert.equal(w.counts.prepare, 0);
  const other = await verified(world()); other.state = null;
  await assert.rejects(output.publishCrate(other.expected, other.ports), /Unclaimed/); assert.equal(other.counts.prepare, 1);
});

test("missing, replaced, corrupted or duplicate verified originals never trigger rebuild or upload", async t => {
  const source = await verified(world());
  for (const [name, change] of [
    ["missing", w => w.assets.clear()],
    ["replacement ID", w => { const item = w.assets.get(1001); item.metadata.id = 1002; w.assets.delete(1001); w.assets.set(1002, item); }],
    ["digest", w => w.assets.get(1001).metadata.digest = "sha256:" + "f".repeat(64)],
    ["raw corruption", w => w.assets.get(1001).bytes[10] ^= 1],
    ["duplicate", w => { const item = w.assets.get(1001); w.assets.set(1002, { bytes: Buffer.from(item.bytes), metadata: { ...item.metadata, id: 1002 } }); }],
    ["wrong size", w => w.assets.get(1001).metadata.size++],
  ]) await t.test(name, async () => {
    const w = world(); w.state = clone(source.state); for (const [id, item] of source.assets) w.assets.set(id, { metadata: clone(item.metadata), bytes: Buffer.from(item.bytes) });
    change(w); await assert.rejects(output.publishCrate(w.expected, w.ports));
    assert.equal(w.counts.prepare, 0); assert.equal(w.counts.registryUpload, 0); assert.equal(w.counts.assetUpload, 0);
  });
});

test("independent expected inputs reject a foreign self-described proof before writes", async t => {
  const source = await verified(world());
  for (const [field, value] of [["sourceRevision", "d".repeat(40)], ["sourceTree", "e".repeat(40)], ["version", "2026.10.28"], ["name", "foreign"], ["releaseId", 8], ["pathInVcs", "elsewhere"]]) {
    await t.test(field, async () => {
      const w = world(); w.state = clone(source.state); w.state.inputs[field] = value;
      await assert.rejects(output.publishCrate(w.expected, w.ports)); assert.equal(w.counts.prepare, 0); assert.equal(w.counts.save, 0);
    });
  }
});

test("source, stamped dirty state, crate layout and embedded frontend are checked from actual tar bytes", () => {
  const value = expected(); assert.equal(output.inspectArchive(fixtureBytes(value), value).name, value.name);
  for (const changes of [{ source: C }, { dirty: false }, { layout: "wrong" }, { extra: ["../escape", "bad"] }]) assert.throws(() => output.inspectArchive(archive(value, changes), value));
  const server = expected("bamboo-server"); output.inspectArchive(fixtureBytes(server), server);
  assert.throws(() => output.inspectArchive(archive(server, { frontend: "different" }), server), /Frontend/);
});

test("outer ZIP is a bounded regular two-file transport and cannot substitute inner archive bytes", async () => {
  const w = await verified(world()), zip = w.assets.get(1001).bytes;
  const actual = output.unseal(zip, w.expected); assert.deepEqual(actual.bytes, w.bytes);
  for (const mode of ["extra", "duplicate", "symlink", "compressed", "archive-drift"]) {
    const modified = python(String.raw`
import io,stat,sys,zipfile
mode=sys.argv[1]; source=zipfile.ZipFile(io.BytesIO(sys.stdin.buffer.read())); dest=io.BytesIO()
with zipfile.ZipFile(dest,'w') as z:
    for item in source.infolist():
        data=source.read(item.filename)
        if mode=='symlink' and item.filename=='package.crate': item.external_attr=(stat.S_IFLNK|0o777)<<16
        if mode=='compressed': item.compress_type=zipfile.ZIP_DEFLATED
        if mode=='archive-drift' and item.filename=='package.crate': data+=b'drift'
        z.writestr(item,data)
    if mode in ('extra','duplicate'):
        item=zipfile.ZipInfo('../extra' if mode=='extra' else 'package.crate'); item.create_system=3; item.external_attr=(stat.S_IFREG|0o644)<<16
        z.writestr(item,b'bad')
sys.stdout.buffer.write(dest.getvalue())
`, zip, [mode]);
    assert.throws(() => output.unseal(modified, w.expected), mode);
  }
});

test("official run, physical numeric job, attempt and complete pages must agree", async t => {
  const source = await verified(world());
  const cases = [
    ["foreign run repository", route => route === "actions/runs/41" ? { ...clone(source.run), repository: { id: 1, full_name: "foreign/repo" } } : undefined],
    ["foreign controller", route => route === "actions/runs/41" ? { ...clone(source.run), head_sha: S } : undefined],
    ["missing total", route => route.includes("/jobs?") ? { jobs: [clone(source.job)] } : undefined],
    ["truncated page", route => route.includes("/jobs?") ? { total_count: 2, jobs: [clone(source.job)] } : undefined],
    ["duplicate job", route => route.includes("/jobs?") ? { total_count: 2, jobs: [clone(source.job), clone(source.job)] } : undefined],
    ["copied job ID", route => route === "actions/jobs/501" ? { ...clone(source.job), id: 502, run_attempt: 2 } : undefined],
    ["wrong attempt", route => route.includes("/jobs?") ? { total_count: 1, jobs: [{ ...clone(source.job), run_attempt: 2 }] } : undefined],
  ];
  for (const [name, hook] of cases) await t.test(name, async () => {
    const w = world(); w.state = clone(source.state); for (const [id, item] of source.assets) w.assets.set(id, { metadata: clone(item.metadata), bytes: Buffer.from(item.bytes) });
    w.readHook = hook; await assert.rejects(output.readVerifiedCrate(w.state, w.expected, w.ports)); assert.equal(w.counts.prepare, 0);
  });
  for (const key of ["GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT"]) await assert.rejects(output.executionOrigin({ ...source.environment, [key]: "1e2" }, source.ports.read));
  await assert.rejects(output.executionOrigin({ ...source.environment, GITHUB_JOB: "501" }, source.ports.read));
});

test("crate A preserves its real failed whole-job outcome after another crate fails", async () => {
  const w = world(); await output.publishCrate(w.expected, w.ports); w.finish("failure");
  const original = await output.readVerifiedCrate(w.state, w.expected, w.ports);
  assert.equal(original.proof.verification.exitCode, 0); assert.equal(original.observedJob.conclusion, "failure"); assert.equal(original.publicationAuthorized, false);
  const closure = await output.readVerifiedClosure([w.expected], [w.state], w.ports);
  assert.equal(closure.crates[0].originalJob.conclusion, "failure"); assert.equal(closure.publicationAuthorized, false);
  await assert.rejects(output.readVerifiedClosure([w.expected, expected("second")], [w.state], w.ports));
});

test("2344 public crate closure keeps the authenticated original origin and registry checksum", async () => {
  const w = world(); await output.publishCrate(w.expected, w.ports); w.finish("failure");
  const closure = await output.readVerifiedClosure([w.expected], [w.state], w.ports);
  assert.equal(closure.publicationAuthorized, false);
  assert.equal(closure.crates[0].registryChecksum, hash(w.bytes));
  assert.deepEqual(closure.crates[0].origin, w.state.origin);
  assert.equal(closure.crates[0].originalJob.conclusion, "failure");
  assert.equal(w.counts.prepare, 1);
  w.ports.registryArchive = async () => Buffer.from("changed registry bytes");
  await assert.rejects(output.readVerifiedClosure([w.expected], [w.state], w.ports), /Registry archive differs/);
});

test("registry checksum, original bytes and published disappearance cannot become a successful resume", async t => {
  for (const mode of ["checksum", "bytes", "missing-published"]) await t.test(mode, async () => {
    const w = mode === "missing-published" ? world() : await verified(world());
    if (mode === "missing-published") await output.publishCrate(w.expected, w.ports);
    if (mode === "checksum") w.published = Buffer.from("foreign");
    if (mode === "bytes") { w.published = w.bytes; w.ports.registryArchive = async () => Buffer.from("foreign"); }
    if (mode === "missing-published") w.published = null;
    const prepared = w.counts.prepare, uploaded = w.counts.registryUpload;
    await assert.rejects(output.publishCrate(w.expected, w.ports)); assert.equal(w.counts.prepare, prepared); assert.equal(w.counts.registryUpload, uploaded);
  });
});

test("sandbox adapter preserves two package operations and one verification compilation", () => {
  const value = expected(), directory = fs.mkdtempSync(path.join(os.tmpdir(), "bamboo-crate-output-unit-")), bytes = fixtureBytes(value);
  const saved = { packageFile: sandbox.packageFile, run: sandbox.run, regular: sandbox.regular }; const calls = [];
  const file = path.join(directory, "original.crate"); fs.writeFileSync(file, bytes);
  try {
    sandbox.packageFile = () => `${value.name}-${value.version}.crate`;
    sandbox.run = (_state, args) => { calls.push([...args]); return 0; };
    sandbox.regular = () => file;
    const prepared = output.prepareFromSandbox({ revision: S, target: directory }, value);
    assert.deepEqual(calls, [["package", "--locked", "--allow-dirty", "--no-verify", "-p", value.name], ["package", "--locked", "--allow-dirty", "-p", value.name]]);
    assert.equal(calls.filter(args => !args.includes("--no-verify")).length, 1); assert.deepEqual(prepared.bytes, bytes);
    assert.deepEqual(prepared.verification.preparationCommand, calls[0]); assert.deepEqual(prepared.verification.command, calls[1]);
    sandbox.run = () => 1; assert.throws(() => output.prepareFromSandbox({ revision: S, target: directory }, value), /preparation did not complete/);
    assert.throws(() => output.prepareFromSandbox({ revision: C, target: directory }, value));
  } finally { Object.assign(sandbox, saved); fs.rmSync(directory, { recursive: true, force: true }); }
});

test("malformed inputs, unsupported phases and verification stubs fail closed", () => {
  for (const change of [{ version: "0.0.0" }, { version: "1.0.0-01" }, { version: "18446744073709551616.1.0" }, { name: "../bad" }, { pathInVcs: "../outside" }, { releaseId: "7" }, { repository: "foreign/repo" }]) assert.throws(() => output.inputs({ ...expected(), ...change }));
  const value = expected(); assert.throws(() => output.record({ phase: "success" }, value));
});

test("authenticated checkpoint read-back is required; a lost successful save response is recoverable", async () => {
  const w = world(), save = w.ports.save;
  w.ports.save = async (next, previous) => { await save(next, previous); throw new Error("Fixture lost PATCH response"); };
  await output.publishCrate(w.expected, w.ports); assert.equal(w.counts.prepare, 1); assert.equal(w.state.phase, "published");
  const broken = world(); broken.ports.save = async () => {};
  await assert.rejects(output.publishCrate(broken.expected, broken.ports), /checkpoint save is unconfirmed/);
  assert.equal(broken.counts.prepare, 0); assert.equal(broken.counts.assetUpload, 0);
});

test("429 budget is bounded and another invocation still uploads the original without repackaging", async () => {
  const w = world(); w.outcomes = Array(25).fill("429");
  await assert.rejects(output.publishCrate(w.expected, w.ports), /unconfirmed/);
  assert.equal(w.counts.registryUpload, 25); assert.equal(w.counts.wait.length, 24); assert.equal(w.state.phase, "verified");
  await output.publishCrate(w.expected, w.ports);
  assert.equal(w.counts.registryUpload, 26); assert.equal(w.counts.prepare, 1); assert.equal(w.counts.assetUpload, 1);
  assert.ok(w.requests.every(bytes => bytes.equals(w.requests[0])));
});

test("input snapshots and upload buffers cannot be replaced across an await", async () => {
  const original = expected(), w = world(), load = w.ports.load;
  w.ports.load = async () => { original.sourceRevision = C; return load(); };
  await output.publishCrate(original, w.ports);
  assert.equal(w.state.inputs.sourceRevision, S);
  const other = await verified(world()), upload = other.ports.upload;
  other.ports.upload = async (bytes, metadata) => { const result = await upload(bytes, metadata); bytes.fill(0); return result; };
  await output.publishCrate(other.expected, other.ports);
  assert.equal(other.state.sealed.proof.archive.sha256, hash(other.bytes));
});

test("all official job pages are consumed before selecting the real current numeric job", async () => {
  const w = world(), prefix = "actions/runs/41/jobs?filter=all&per_page=100&page=";
  w.readHook = route => route === prefix + "1" ? { total_count: 101, jobs: Array.from({ length: 100 }, (_value, i) => ({ id: 1000 + i, name: "Other", run_attempt: 1 })) }
    : route === prefix + "2" ? { total_count: 101, jobs: [clone(w.job)] } : undefined;
  const value = await output.executionOrigin(w.environment, w.ports.read); assert.equal(value.jobId, 501);
  assert.ok(w.routes.includes(prefix + "2"));
  w.readHook = route => route === prefix + "1" ? { total_count: 101, jobs: Array.from({ length: 100 }, (_value, i) => ({ id: 1000 + i })) }
    : route === prefix + "2" ? { total_count: 102, jobs: [clone(w.job)] } : undefined;
  await assert.rejects(output.executionOrigin(w.environment, w.ports.read));
});

test("a verification stub or failed sandbox result cannot be sealed or uploaded", async () => {
  for (const change of [verification => { verification.exitCode = 1; }, verification => { verification.command.splice(1, 0, "--no-verify"); },
    verification => { verification.preparationCommand = verification.command; }, verification => { verification.image = "rust:latest"; }]) {
    const w = world(), prepare = w.ports.prepare;
    w.ports.prepare = async item => { const result = await prepare(item); change(result.verification); return result; };
    await assert.rejects(output.publishCrate(w.expected, w.ports));
    assert.equal(w.state.phase, "preparing"); assert.equal(w.state.sealed, undefined); assert.equal(w.counts.assetUpload, 0); assert.equal(w.counts.registryUpload, 0);
  }
});

test("controller pre-verification hash rejects a worker changing both archive and extracted tree", () => {
  const value = expected(), bytes = fixtureBytes(value), replaced = archive(value, { sourceText: 'compile_error!("not the original");\n' });
  output.inspectArchive(replaced, value); // Still valid, with the same source/name/version metadata.
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "bamboo-crate-preverify-hash-")), file = path.join(directory, "original.crate");
  const saved = { packageFile: sandbox.packageFile, run: sandbox.run, regular: sandbox.regular }; let calls = 0;
  try {
    fs.writeFileSync(file, bytes); sandbox.packageFile = () => `${value.name}-${value.version}.crate`; sandbox.regular = () => file;
    sandbox.run = (_state, args) => {
      calls++;
      if (!args.includes("--no-verify")) {
        fs.writeFileSync(file, replaced);
        fs.mkdirSync(path.join(directory, "extracted/src"), { recursive: true });
        fs.writeFileSync(path.join(directory, "extracted/src/lib.rs"), 'compile_error!("not the original");\n');
      }
      return 0;
    };
    assert.throws(() => output.prepareFromSandbox({ revision: S, target: directory }, value), /controller's original bytes/);
    assert.equal(calls, 2);
  } finally { Object.assign(sandbox, saved); fs.rmSync(directory, { recursive: true, force: true }); }
});

test("explicit upload and fresh guard ports are required; bare tokens never select a default", async t => {
  for (const [name, field, message] of [["missing guard", "guard", /Fresh source guard/], ["no upload port", "upload", /Explicit archive upload port/]]) {
    await t.test(name, async () => {
      const w = world(); let bareTokenReads = 0;
      Object.defineProperty(w.ports, "registryToken", { get: () => { bareTokenReads++; throw new Error("Fixture forbids the bare-token fallback before any network call"); } });
      delete w.ports[field];
      await assert.rejects(output.publishCrate(w.expected, w.ports), message);
      assert.equal(bareTokenReads, 0); assert.equal(w.counts.prepare, 0); assert.equal(w.counts.assetUpload, 0);
      assert.equal(w.counts.save, 0); assert.equal(w.counts.registryUpload, 0);
    });
  }
});

test("429 retry stops before a second PUT when the fresh source guard refuses", async () => {
  const w = world(); w.outcomes = ["429", "ok"];
  w.ports.guard = async () => { if (++w.counts.guard === 2) throw new Error("Fixture source changed during the 429 wait"); };
  await assert.rejects(output.publishCrate(w.expected, w.ports), /source changed during the 429 wait/);
  assert.equal(w.counts.guard, 2); assert.equal(w.counts.registryUpload, 1); assert.equal(w.requests.length, 1);
  assert.equal(w.state.phase, "verified"); assert.equal(w.counts.prepare, 1); assert.equal(w.counts.assetUpload, 1);
  assert.equal(w.counts.wait.length, 1); assert.equal(w.published, null);
});

test("restored output requires a fresh source guard before its next PUT", async () => {
  const w = await verified(world()), original = clone(w.state);
  w.ports.guard = async () => { w.counts.guard++; throw new Error("Fixture source advanced before resume"); };
  await assert.rejects(output.publishCrate(w.expected, w.ports), /source advanced before resume/);
  assert.equal(w.counts.guard, 2); assert.equal(w.counts.registryUpload, 1); assert.equal(w.requests.length, 1);
  assert.equal(w.counts.prepare, 1); assert.equal(w.counts.assetUpload, 1); assert.deepEqual(w.state, original);
});
