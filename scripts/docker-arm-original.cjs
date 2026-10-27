#!/usr/bin/env node
"use strict";
// Docker-only additional target. No standard Release writer, signer or allocator.
const assert = require("node:assert/strict"), fs = require("node:fs"), path = require("node:path");
const { spawnSync } = require("node:child_process");
const consumer = require("./docker-from-release.cjs"), core = require("./crate-release.cjs");
const sidecar = require("./sidecar-release.cjs"), source = require("./frozen-source-admission.cjs");
const build = require("./sidecar-build.cjs"), ROOT = path.resolve(__dirname, "..");
// Official index/config/Dockerfile metadata: #2353 read-only handoff, 2026-10-27.
const BUILDER = "docker.io/library/rust:1.99.1-bookworm@sha256:32f7944ed00949c44d3944a0a852b3e5ea862a610a38b43ac92a33aa87256b75";
const CONFIG = "sha256:288b8fa317be20e10d5784cf65883a3b44977e2b0202635b9658970bba88d570";
const BUILDER_MANIFEST = BUILDER.split("@")[1], REPOSITORY_DIGEST = "rust@" + BUILDER_MANIFEST;
const TARGET = "aarch64-unknown-linux-gnu", KIND = "bamboo-docker-arm-original/v1", PREFIX = "Bamboo Docker ARM original: ";
const SELECT = "Select ARM executable original", PREPARE = "Prepare frozen ARM source and dependencies";
const COMPILE = "Compile the sole versioned ARM binary", CAPTURE = "Verify ARM binary and record receipt";
const UPLOAD = consumer.ARM_UPLOAD, SEAL = "Authenticate retained ARM original", REUSE = "Verify retained ARM executable original";
const PRODUCTION = [PREPARE, COMPILE, CAPTURE, UPLOAD, SEAL];
const MAX_ZIP = 1024 ** 3, MAX_BINARY = MAX_ZIP - 8 * 1024 ** 2, MAX_RECEIPT = 65536, MAX_MESSAGES = 64 * 1024 ** 2;
const ARGS = Object.freeze(["build", "--locked", "--offline", "--release", "-p", "bamboo-agent", "--bin", "bamboo", "--target", TARGET, "--message-format=json-render-diagnostics"]);
const { step, passed, skipped } = consumer, sha = bytes => core.sha256(bytes);
const keys = (value, names) => assert.deepEqual(Object.keys(value).sort(), [...names].sort());
const hash = value => assert.match(value, /^[0-9a-f]{64}$/);
const positive = value => { assert.ok(Number.isSafeInteger(Number(value)) && /^[1-9][0-9]*$/.test(String(value))); return Number(value); };
const artifactName = (run, attempt) => `docker-arm-original-${positive(run)}-${positive(attempt)}`;
function layout() {
  assert.ok(path.isAbsolute(process.env.RUNNER_TEMP || ""));
  const root = path.join(fs.realpathSync(process.env.RUNNER_TEMP), "bamboo-arm-original");
  assert.doesNotMatch(root, /[,\r\n]/);
  return Object.fromEntries([["root", root], ...["source", "target", "cargo", "home", "out"].map(name => [name, path.join(root, name)])]);
}
function childEnv(home) {
  return { PATH: process.env.PATH, HOME: home, TMPDIR: process.env.RUNNER_TEMP, LANG: "C.UTF-8",
    GIT_CONFIG_NOSYSTEM: "1", GIT_CONFIG_GLOBAL: "/dev/null", GIT_TERMINAL_PROMPT: "0", GIT_OPTIONAL_LOCKS: "0" };
}
function command(program, args, options = {}) {
  const result = spawnSync(program, args, { env: childEnv(layout().home), encoding: "utf8", maxBuffer: MAX_MESSAGES, timeout: 120000, ...options });
  assert.ok(!result.error && result.status === 0, "ARM original command failed: " + program); return result.stdout;
}
function git(work, args) { return command("git", ["-c", "core.fsmonitor=false", "-c", "core.hooksPath=/dev/null", "-C", work.source, ...args]).trim(); }
function containerArgs(work, phase, args) {
  assert.ok(["prepare", "compile", "inspect"].includes(phase));
  const mounts = ["source", "target", "cargo", "home"].flatMap(name => ["--mount", `type=bind,source=${work[name]},target=${work[name]}` +
    (name === "source" && phase !== "prepare" || phase === "inspect" && name !== "home" ? ",readonly" : "")]);
  return ["run", "--rm", "--init", "--platform", "linux/arm64", "--network", phase === "prepare" ? "bridge" : "none",
    "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--user", `${process.getuid()}:${process.getgid()}`,
    ...mounts, "--mount", `type=bind,source=${work.source}/.git,target=${work.source}/.git,readonly`,
    "--workdir", work.source, "--env", "HOME=" + work.home, "--env", "CARGO_HOME=" + work.cargo,
    "--env", "RUSTUP_HOME=/usr/local/rustup", "--env", "RUSTUP_TOOLCHAIN=1.99.1", "--env", "CARGO_BUILD_JOBS=4",
    "--env", "BAMBOO_FRONTEND_BUILD_MODE=embedded", "--env", "CARGO_TARGET_DIR=" + work.target,
    "--entrypoint", args[0], BUILDER, ...args.slice(1)];
}
function validateBuilderStore(value) {
  keys(value, ["role", "id", "repositoryDigest", "descriptor"]);
  assert.equal(value.repositoryDigest, REPOSITORY_DIGEST);
  assert.ok(["config", "manifest"].includes(value.role));
  assert.equal(value.id, value.role === "config" ? CONFIG : BUILDER_MANIFEST, "Unknown Docker store identity");
  if (value.descriptor !== null) {
    keys(value.descriptor, ["digest", "mediaType", "size"]);
    assert.deepEqual(value.descriptor, { digest: BUILDER_MANIFEST, mediaType: "application/vnd.oci.image.manifest.v1+json", size: 1942 });
  }
  if (value.role === "manifest") assert.ok(value.descriptor, "Manifest store requires its exact single-ARM descriptor");
  return value;
}
function checkBuilder(image) {
  assert.equal(image.Os, "linux"); assert.equal(image.Architecture, "arm64");
  assert.deepEqual(image.Config.Env.filter(value => value.startsWith("RUST_VERSION=")), ["RUST_VERSION=1.99.1"]);
  assert.ok(Array.isArray(image.RepoDigests) && image.RepoDigests.includes(REPOSITORY_DIGEST));
  assert.ok([CONFIG, BUILDER_MANIFEST].includes(image.Id), "Unknown Docker image ID/index");
  // Classic stores expose the config ID; containerd stores expose the manifest
  // ID. Both must bind this one pinned ARM manifest/config pair, never an index.
  return validateBuilderStore({ role: image.Id === CONFIG ? "config" : "manifest", id: image.Id, repositoryDigest: REPOSITORY_DIGEST,
    descriptor: image.Descriptor == null ? null : { digest: image.Descriptor.digest, mediaType: image.Descriptor.mediaType, size: image.Descriptor.size } });
}
function frontend(facts) {
  assert.equal(facts.frontend.length, 2);
  return ["frontend-manifest.json", "lotus-frontend.zip"].map((name, index) => {
    const ref = facts.frontend[index]; assert.equal(ref.name, name); hash(ref.sha256); positive(ref.assetId);
    assert.ok(Number.isSafeInteger(ref.sizeBytes) && ref.sizeBytes > 0 && ref.sizeBytes <= (index ? 16 : 1) * 1024 ** 2);
    return { name, assetId: ref.assetId, sha256: ref.sha256, sizeBytes: ref.sizeBytes };
  });
}
function binding(state, context, physical) {
  return { repository: sidecar.REPOSITORY, controller: state.controller, source: state.facts.source.revision,
    tree: state.facts.source.tree, standard: consumer.selected(state.facts), completionSha256: state.facts.completionSha256,
    frontend: frontend(state.facts), runId: context.run.id, jobId: physical.id, physicalAttempt: physical.run_attempt };
}
function validateReceipt(value, state, context, physical) {
  keys(value, ["schemaVersion", "kind", "identity", "build", "binary", "abi"]);
  assert.equal(value.schemaVersion, 1); assert.equal(value.kind, KIND); assert.deepEqual(value.identity, binding(state, context, physical));
  const b = value.build;
  keys(b, ["command", "target", "features", "profile", "frontendMode", "builder", "pinnedBuilderConfig", "builderStore", "rustc", "cargo", "versionOutput",
    "sourceLockSha256", "stampedLockSha256", "stampedDiffSha256", "stampedPaths", "metadataSha256", "cargoMessagesSha256"]);
  assert.deepEqual(b.command, ["cargo", ...ARGS]); assert.equal(b.target, TARGET); assert.deepEqual(b.features, []);
  assert.equal(b.profile, "release"); assert.equal(b.frontendMode, "embedded"); assert.equal(b.builder, BUILDER); assert.equal(b.pinnedBuilderConfig, CONFIG);
  validateBuilderStore(b.builderStore);
  assert.match(b.rustc, /^release: 1\.99\.1$/m); assert.match(b.rustc, /^host: aarch64-unknown-linux-gnu$/m);
  assert.match(b.cargo, /^cargo 1\.99\.\d+ /); assert.equal(b.versionOutput, "bamboo " + state.facts.release.version);
  for (const name of ["sourceLockSha256", "stampedLockSha256", "stampedDiffSha256", "metadataSha256", "cargoMessagesSha256"]) hash(b[name]);
  assert.ok(Array.isArray(b.stampedPaths) && b.stampedPaths.length && b.stampedPaths.length <= 64);
  assert.deepEqual([...new Set(b.stampedPaths)].sort(), b.stampedPaths);
  for (const p of b.stampedPaths) assert.match(p, /^(?:Cargo\.(?:toml|lock)|crates\/[a-z-]+\/[a-z-]+\/Cargo\.toml)$/);
  keys(value.binary, ["sha256", "sizeBytes"]); hash(value.binary.sha256);
  assert.ok(Number.isSafeInteger(value.binary.sizeBytes) && value.binary.sizeBytes >= 65536 && value.binary.sizeBytes <= MAX_BINARY);
  keys(value.abi, ["needed", "symbolVersions", "interpreter"]); assert.equal(value.abi.interpreter, "/lib/ld-linux-aarch64.so.1");
  assert.ok(Array.isArray(value.abi.needed) && value.abi.needed.length <= 64 && Array.isArray(value.abi.symbolVersions) && value.abi.symbolVersions.length <= 256);
  for (const name of value.abi.needed) assert.match(name, /^[A-Za-z0-9._+-]+$/);
  for (const v of value.abi.symbolVersions) assert.match(v, /^(?:GLIBC|GLIBCXX|CXXABI)_[0-9.]+$/);
  consumer.elfReport("Class: ELF64\nMachine: AArch64\n" + value.abi.needed.map(name => "(NEEDED) [" + name + "]").join("\n") + "\n" + value.abi.symbolVersions.join(" "), "linux/arm64");
  return value;
}
function history(jobs, run, beforeAttempt = run.run_attempt + 1, final = false) {
  const family = jobs.filter(job => job.name === consumer.ARM_JOB && job.run_attempt < beforeAttempt);
  assert.equal(new Set(family.map(job => job.run_attempt)).size, family.length, "Ambiguous ARM attempt");
  const originals = new Map();
  for (const job of family) {
    consumer.jobHeader(job, run); assert.equal(job.status, "completed", "Unknown active ARM compilation");
    const compile = step(job, COMPILE);
    if (compile.conclusion === "success") {
      passed(job); for (const name of [SELECT, ...PRODUCTION]) passed(step(job, name)); skipped(step(job, REUSE));
      const key = source.executionKey(job); assert.ok(key);
      if (!originals.has(key) || job.run_attempt < originals.get(key).run_attempt) originals.set(key, job);
    } else {
      // Any failure/cancellation/unknown compiler outcome may already contain a
      // completed output. A missing checkpoint never authorizes another build.
      skipped(compile); for (const name of [CAPTURE, UPLOAD, SEAL]) skipped(step(job, name));
      assert.equal(step(job, REUSE).status, "completed");
    }
  }
  assert.ok(originals.size <= 1, "More than one physical ARM compilation");
  const physical = [...originals.values()][0], latest = [...family].sort((a,b) => b.run_attempt - a.run_attempt)[0];
  if (family.some(job => step(job, REUSE).conclusion !== "skipped")) assert.ok(physical, "ARM reuse lost its original; STOP");
  if (final) {
    assert.ok(physical && latest, "No completed ARM original"); passed(latest);
    if (step(latest, COMPILE).conclusion !== "success") {
      passed(step(latest, SELECT)); passed(step(latest, REUSE)); for (const name of PRODUCTION) skipped(step(latest, name));
    }
  }
  return { physical, latest };
}
async function noOrphan(context) {
  const pattern = new RegExp("^docker-arm-original-" + positive(context.run.id) + "-[1-9][0-9]*$");
  const rows = await context.paged(`actions/runs/${context.run.id}/artifacts`, "artifacts");
  assert.ok(!rows.some(value => pattern.test(value.name)), "Orphan/expired ARM original; never compile again");
}
const ZIP_READER = String.raw`import io,json,stat,sys,zipfile
raw=sys.stdin.buffer.read(1024**3+1); assert 0<len(raw)<=1024**3
with zipfile.ZipFile(io.BytesIO(raw)) as archive:
 items=archive.infolist(); assert len(items)==2 and sorted(x.filename for x in items)==['bamboo','receipt.json']
 for item in items:
  assert not item.is_dir() and not item.flag_bits&1 and stat.S_IFMT(item.external_attr>>16) in (0,stat.S_IFREG)
  limit=65536 if item.filename=='receipt.json' else 1024**3-8*1024**2
  assert 0<item.file_size<=limit
  if item.filename=='receipt.json': receipt=archive.read(item); assert len(receipt)==item.file_size
  else:
   assert item.file_size>=65536
   with archive.open(item) as src,open(sys.argv[1],'xb') as dst:
    total=0
    while True:
     block=src.read(1024*1024)
     if not block: break
     total+=len(block); assert total<=item.file_size; dst.write(block)
    assert total==item.file_size
sys.stdout.buffer.write(receipt)`;
function decodeOriginal(bytes, destination) {
  assert.ok(Buffer.isBuffer(bytes) && bytes.length > 0 && bytes.length <= MAX_ZIP);
  const receipt = command("python3", ["-I", "-c", ZIP_READER, destination], { input: bytes, encoding: null, maxBuffer: MAX_RECEIPT });
  const value = JSON.parse(receipt.toString("utf8")); assert.equal(receipt.toString("utf8"), JSON.stringify(value, null, 2) + "\n");
  return { receipt: value, receiptSha256: sha(receipt) };
}
function completion(state, context, physical, artifact, record) {
  return { ...binding(state, context, physical), artifactId: artifact.id, artifactDigest: artifact.digest,
    receiptSha256: record.receiptSha256, binary: record.receipt.binary };
}
function readFrame(log, physical) {
  assert.ok(typeof log === "string" && Buffer.byteLength(log) <= MAX_MESSAGES);
  const rows = log.split(/\r?\n/).map(line => line.match(/^(\d{4}-\d\d-\d\dT\S+Z) (.*)$/)).filter(value => value && value[2].startsWith(PREFIX));
  assert.equal(rows.length, 1, "Missing/ambiguous ARM completion frame");
  const seal = step(physical, SEAL), time = Date.parse(rows[0][1]); passed(seal);
  assert.ok(Number.isFinite(time) && Math.floor(Date.parse(seal.started_at) / 1000) <= Math.floor(time / 1000) && Math.floor(time / 1000) <= Math.floor(Date.parse(seal.completed_at) / 1000));
  const encoded = rows[0][2].slice(PREFIX.length); assert.ok(encoded.length <= MAX_RECEIPT && /^[A-Za-z0-9+/]+={0,2}$/.test(encoded));
  const bytes = Buffer.from(encoded, "base64"), result = JSON.parse(bytes.toString("utf8"));
  assert.equal(bytes.toString("base64"), encoded); assert.equal(core.canonicalJson(result), bytes.toString("utf8")); return result;
}
async function loadOriginal(state, context, physical, sealing = false) {
  if (!sealing) passed(physical);
  for (const name of [SELECT, PREPARE, COMPILE, CAPTURE, UPLOAD]) passed(step(physical, name));
  const direct = await context.get("actions/jobs/" + physical.id); assert.equal(source.fingerprint(direct), source.fingerprint(physical)); assert.equal(direct.run_attempt, physical.run_attempt);
  const attempts = await context.paged(`actions/runs/${context.run.id}/attempts/${physical.run_attempt}/jobs`, "jobs");
  const origin = attempts.filter(job => job.id === physical.id); assert.equal(origin.length, 1); assert.equal(source.fingerprint(origin[0]), source.fingerprint(physical)); assert.equal(origin[0].run_attempt, physical.run_attempt);
  const rows = await context.paged(`actions/runs/${context.run.id}/artifacts`, "artifacts");
  assert.equal(rows.filter(value => new RegExp("^docker-arm-original-" + positive(context.run.id) + "-[1-9][0-9]*$").test(value.name)).length, 1, "Unexpected ARM original carrier");
  const matches = rows.filter(value => value.name === artifactName(context.run.id, physical.run_attempt)); assert.equal(matches.length, 1, "ARM original missing/ambiguous; STOP");
  const artifact = matches[0]; positive(artifact.id); assert.equal(artifact.expired, false); assert.match(artifact.digest, /^sha256:[0-9a-f]{64}$/);
  assert.ok(Number.isSafeInteger(artifact.size_in_bytes) && artifact.size_in_bytes > 0 && artifact.size_in_bytes <= MAX_ZIP);
  assert.deepEqual(artifact.workflow_run, { ...artifact.workflow_run, id: context.run.id, head_sha: context.run.head_sha, head_branch: context.run.head_branch });
  const upload = step(physical, UPLOAD), second = value => Math.floor(Date.parse(value) / 1000);
  assert.ok(Number.isSafeInteger(second(artifact.created_at)) && second(upload.started_at) <= second(artifact.created_at) && second(artifact.created_at) <= second(upload.completed_at));
  // Official job logs need not exist while this job is still running. Seal
  // binds API/action outputs now; the completed-job reader authenticates the
  // physical upload log and completion frame independently before consumption.
  const log = sealing ? null : await state.reads.jobLog(physical.id);
  if (!sealing) consumer.physicalUpload(log, physical, artifact, context.run.id, UPLOAD);
  const bytes = Buffer.from(await state.reads.artifactBytes(artifact.id, artifact.size_in_bytes));
  assert.equal(bytes.length, artifact.size_in_bytes); assert.equal("sha256:" + sha(bytes), artifact.digest);
  const directory = fs.mkdtempSync(path.join(fs.realpathSync(process.env.RUNNER_TEMP), "docker-arm-read-")), binaryFile = path.join(directory, "bamboo");
  const record = decodeOriginal(bytes, binaryFile); validateReceipt(record.receipt, state, context, physical);
  assert.deepEqual(sidecar.fileIdentity(binaryFile, MAX_BINARY, 65536), record.receipt.binary);
  const fd = fs.openSync(binaryFile, "r"), header = Buffer.alloc(20); try { assert.equal(fs.readSync(fd, header, 0, 20, 0), 20); } finally { fs.closeSync(fd); }
  assert.equal(header.subarray(0, 6).toString("hex"), "7f454c460201"); assert.equal(header.readUInt16LE(18), 183); assert.ok([2,3].includes(header.readUInt16LE(16)));
  if (!sealing) assert.deepEqual(readFrame(log, physical), completion(state, context, physical, artifact, record));
  const fields = value => [value.id, value.name, value.expired, value.size_in_bytes, value.digest, value.created_at, value.workflow_run];
  assert.deepEqual(fields(await context.get("actions/artifacts/" + artifact.id)), fields(artifact));
  const fresh = await context.get("actions/jobs/" + physical.id); assert.equal(fresh.run_attempt, physical.run_attempt); assert.equal(source.fingerprint(fresh), source.fingerprint(physical));
  return { binaryFile, ...record, artifact, reference: { kind: KIND, target: TARGET, ...record.receipt.binary,
    runId: context.run.id, jobId: physical.id, physicalAttempt: physical.run_attempt, artifactId: artifact.id, artifactDigest: artifact.digest, receiptSha256: record.receiptSha256 } };
}
async function readOriginal(state, context) {
  await consumer.crossRunGuard(state, context);
  const physical = history(context.jobs, context.run, context.run.run_attempt + 1, true).physical;
  return loadOriginal(state, context, physical);
}
function sourceState(work) {
  assert.equal(git(work, ["ls-files", "--others", "--exclude-standard"]), ""); assert.equal(git(work, ["diff", "--cached", "--name-only"]), "");
  return { paths: git(work, ["diff", "--no-ext-diff", "--no-textconv", "--name-only"]).split("\n").filter(Boolean).sort(),
    diffSha256: sha(command("git", ["-c", "core.fsmonitor=false", "-C", work.source, "diff", "--no-ext-diff", "--no-textconv", "--binary"], { encoding: null })) };
}
const writeJSON = (file, value) => fs.writeFileSync(file, JSON.stringify(value, null, 2) + "\n", { flag: "wx", mode: 0o600 });
function manifestHashes(work, manifests) {
  assert.ok(Array.isArray(manifests) && manifests.length && manifests.length <= 64);
  assert.equal(new Set(manifests).size, manifests.length);
  return Object.fromEntries(manifests.map(file => {
    assert.match(file, /^(?:Cargo\.toml|crates\/[a-z-]+\/[a-z-]+\/Cargo\.toml)$/);
    const full = path.join(work.source, file); assert.equal(fs.realpathSync(full), full, "Stamped input escaped its ordinary source path");
    assert.ok(fs.lstatSync(full).isFile() && !fs.lstatSync(full).isSymbolicLink());
    return [file, sidecar.fileIdentity(full, 1024 ** 2).sha256];
  }));
}
function stampInputs(work) {
  const paths = ["Cargo.toml"], crates = path.join(work.source, "crates");
  assert.equal(fs.realpathSync(crates), crates);
  for (const category of fs.readdirSync(crates, { withFileTypes: true })) {
    assert.ok(!category.isSymbolicLink(), "Stamper category is a link"); if (!category.isDirectory()) continue;
    for (const member of fs.readdirSync(path.join(crates, category.name), { withFileTypes: true })) {
      assert.ok(!member.isSymbolicLink(), "Stamper crate is a link"); if (!member.isDirectory()) continue;
      const file = `crates/${category.name}/${member.name}/Cargo.toml`;
      if (fs.existsSync(path.join(work.source, file))) paths.push(file);
    }
  }
  manifestHashes(work, paths); return paths.sort();
}
function prepared(work, state, context) {
  const value = JSON.parse(fs.readFileSync(path.join(work.root, "prepared.json"), "utf8"));
  assert.deepEqual(value.identity, binding(state, context, context.current)); assert.equal(git(work, ["rev-parse", "HEAD"]), state.facts.source.revision);
  assert.equal(git(work, ["rev-parse", "HEAD^{tree}"]), state.facts.source.tree); assert.deepEqual(sourceState(work), value.stamped);
  const metadataBytes = fs.readFileSync(path.join(work.root, "metadata.json")); assert.equal(sha(metadataBytes), value.metadataSha256);
  const server = build.verifyStampedVersions(JSON.parse(metadataBytes), state.facts.release.version, { root: work.source, manifestPaths: value.manifests });
  return { ...value, server };
}
async function prepare(state, context, work) {
  assert.equal(process.platform, "linux"); assert.equal(process.arch, "arm64"); assert.equal(command("uname", ["-m"]).trim(), "aarch64");
  fs.mkdirSync(work.root, { mode: 0o700 }); for (const name of ["source", "target", "cargo", "home", "out"]) fs.mkdirSync(work[name], { mode: 0o700 });
  command("git", ["init", work.source]); git(work, ["fetch", "--depth=1", "--no-tags", "https://github.com/" + sidecar.REPOSITORY + ".git", state.facts.source.revision]); git(work, ["checkout", "--detach", "FETCH_HEAD"]);
  assert.equal(git(work, ["rev-parse", "HEAD"]), state.facts.source.revision); assert.equal(git(work, ["rev-parse", "HEAD^{tree}"]), state.facts.source.tree);
  const sourceLockSha256 = sha(fs.readFileSync(path.join(work.source, "Cargo.lock")));
  for (const ref of frontend(state.facts)) {
    const file = path.join(work.source, "crates/app/bamboo-server/frontend_package", ref.name);
    assert.equal(fs.realpathSync(file), file); assert.ok(fs.lstatSync(file).isFile() && !fs.lstatSync(file).isSymbolicLink());
    const bytes = await consumer.original(state.reads, ref); assert.equal(sha(fs.readFileSync(file)), ref.sha256); fs.writeFileSync(file, bytes);
  }
  assert.deepEqual(sourceState(work).paths, []);
  command(process.execPath, [path.join(ROOT, "scripts/frontend-package.cjs"), "verify", "--source-root", work.source]);
  const candidates = stampInputs(work); // validate every Python glob path before C reads or writes S
  assert.deepEqual(git(work, ["ls-files", "--", "Cargo.toml", ":(glob)crates/*/*/Cargo.toml"]).split("\n").sort(), candidates);
  const stamper = path.join(ROOT, "scripts/crate-release-lock.py");
  const manifests = JSON.parse(command("python3", ["-I", stamper, "manifests"], { cwd: work.source }));
  for (const file of manifests) assert.ok(candidates.includes(file));
  command("python3", ["-I", stamper, "stamp", state.facts.release.version], { cwd: work.source });
  const stampedManifests = manifestHashes(work, manifests);
  const inspect = args => command("docker", containerArgs(work, "prepare", args), { timeout: 20 * 60000 });
  assert.equal(inspect(["uname", "-m"]).trim(), "aarch64");
  const builderStore = checkBuilder(JSON.parse(command("docker", ["image", "inspect", BUILDER, "--format", "{{json .}}"], { maxBuffer: 1024 ** 2 })));
  const rustc = inspect(["rustc", "-Vv"]).trim(), cargo = inspect(["cargo", "-V"]).trim();
  assert.match(rustc, /^release: 1\.99\.1$/m); assert.match(rustc, /^host: aarch64-unknown-linux-gnu$/m);
  inspect(["/bin/sh", "-ec", "command -v cc; command -v ar; command -v readelf; command -v pkg-config; pkg-config --modversion openssl"]);
  inspect(["cargo", "metadata", "--format-version", "1"]); // only resolves stamped local versions; never compiles
  command("python3", ["-I", stamper], { cwd: work.source }); // every external lock coordinate/checksum unchanged
  const metadataBytes = Buffer.from(inspect(["cargo", "metadata", "--locked", "--no-deps", "--format-version", "1"]));
  fs.writeFileSync(path.join(work.root, "metadata.json"), metadataBytes, { flag: "wx" });
  build.verifyStampedVersions(JSON.parse(metadataBytes), state.facts.release.version, { root: work.source, manifestPaths: manifests });
  inspect(["cargo", "fetch", "--locked", "--target", TARGET]);
  assert.deepEqual(manifestHashes(work, manifests), stampedManifests, "Dependency preparation changed a stamped manifest");
  const stamped = sourceState(work), allowed = new Set([...manifests, "Cargo.lock"]);
  for (const file of stamped.paths) assert.ok(allowed.has(file), "Unexpected stamped source path");
  writeJSON(path.join(work.root, "prepared.json"), { identity: binding(state, context, context.current), manifests, stamped, rustc, cargo, builderStore,
    sourceLockSha256, stampedLockSha256: sha(fs.readFileSync(path.join(work.source, "Cargo.lock"))), metadataSha256: sha(metadataBytes) });
}
function capture(state, context, work) {
  const data = prepared(work, state, context), log = path.join(work.root, "cargo.jsonl"); assert.ok(fs.statSync(log).size <= MAX_MESSAGES);
  const raw = fs.readFileSync(log), messages = raw.toString("utf8").trim().split("\n").map(line => JSON.parse(line));
  const binary = path.join(work.target, TARGET, "release/bamboo"), refs = frontend(state.facts);
  build.verifyBuildMessages(messages, { binary, targetDirectory: work.target, serverPackageId: data.server.id,
    frontendBytes: { manifestSha256: refs[0].sha256, archiveSha256: refs[1].sha256 } });
  assert.equal(messages.find(item => item.reason === "compiler-artifact" && item.executable === binary)?.fresh, false, "Require this sole fresh product build");
  const inspect = args => command("docker", containerArgs(work, "inspect", args));
  const report = inspect(["readelf", "--wide", "--file-header", "--program-headers", "--dynamic", "--version-info", binary]);
  const abi = { ...consumer.elfReport(report, "linux/arm64"), interpreter: report.match(/Requesting program interpreter: ([^\]]+)\]/)?.[1] };
  assert.equal(abi.interpreter, "/lib/ld-linux-aarch64.so.1");
  const versionOutput = inspect([binary, "--version"]).trim(); assert.equal(versionOutput, "bamboo " + state.facts.release.version);
  assert.deepEqual(sourceState(work), data.stamped);
  const value = { schemaVersion: 1, kind: KIND, identity: data.identity, binary: sidecar.fileIdentity(binary, MAX_BINARY, 65536), abi,
    build: { command: ["cargo", ...ARGS], target: TARGET, features: [], profile: "release", frontendMode: "embedded", builder: BUILDER, pinnedBuilderConfig: CONFIG, builderStore: data.builderStore,
      rustc: data.rustc, cargo: data.cargo, versionOutput, sourceLockSha256: data.sourceLockSha256, stampedLockSha256: data.stampedLockSha256,
      stampedDiffSha256: data.stamped.diffSha256, stampedPaths: data.stamped.paths, metadataSha256: data.metadataSha256, cargoMessagesSha256: sha(raw) } };
  validateReceipt(value, state, context, context.current); const receipt = Buffer.from(JSON.stringify(value, null, 2) + "\n"); assert.ok(receipt.length <= MAX_RECEIPT);
  fs.copyFileSync(binary, path.join(work.out, "bamboo"), fs.constants.COPYFILE_EXCL); fs.writeFileSync(path.join(work.out, "receipt.json"), receipt, { flag: "wx" });
}
async function main() {
  assert.equal(process.argv.length, 3); const phase = process.argv[2], names = { select: SELECT, prepare: PREPARE, compile: COMPILE, capture: CAPTURE, seal: SEAL, reuse: REUSE };
  assert.ok(Object.hasOwn(names, phase), "Unknown ARM phase");
  const state = await consumer.authenticated(), context = await consumer.consumerExecution(state, "arm-originals");
  assert.equal(step(context.current, names[phase]).status, "in_progress"); await consumer.crossRunGuard(state, context);
  const prior = history(context.jobs, context.run, context.run.run_attempt).physical;
  if (phase === "select" || phase === "reuse") {
    if (prior) { await loadOriginal(state, context, prior); fs.appendFileSync(process.env.GITHUB_OUTPUT, "mode=reuse\n"); }
    else { assert.equal(phase, "select", "No original exists for reuse"); await noOrphan(context); fs.appendFileSync(process.env.GITHUB_OUTPUT, "mode=build\n"); }
    if (phase === "reuse") for (const name of PRODUCTION) skipped(step(context.current, name));
    return;
  }
  assert.ok(!prior, "Successful ARM original cannot be replaced");
  if (phase !== "seal") await noOrphan(context);
  passed(step(context.current, SELECT));
  const work = layout();
  if (phase === "prepare") return prepare(state, context, work);
  passed(step(context.current, PREPARE));
  if (phase === "compile") {
    prepared(work, state, context);
    assert.ok(fs.readdirSync(work.target).every(name => [".rustc_info.json", "CACHEDIR.TAG"].includes(name)), "Owned ARM target contains previous compiler outputs");
    await consumer.refreshPublication(state); await consumer.freshConsumer(context, COMPILE);
    const fd = fs.openSync(path.join(work.root, "cargo.jsonl"), "wx");
    try { command("docker", containerArgs(work, "compile", ["cargo", ...ARGS]), { encoding: null, stdio: ["ignore", fd, "inherit"], timeout: 90 * 60000 }); }
    finally { fs.closeSync(fd); } return;
  }
  passed(step(context.current, COMPILE));
  if (phase === "capture") return capture(state, context, work);
  assert.equal(phase, "seal"); passed(step(context.current, CAPTURE));
  // Upload is already present in this phase, so noOrphan applies only above
  // before upload; seal authenticates that unique official original instead.
  const result = await loadOriginal(state, context, context.current, true);
  assert.equal(positive(process.env.ARM_ARTIFACT_ID), result.artifact.id);
  assert.equal("sha256:" + process.env.ARM_ARTIFACT_DIGEST, result.artifact.digest);
  assert.equal(result.receiptSha256, sha(fs.readFileSync(path.join(work.out, "receipt.json"))));
  assert.deepEqual(sidecar.fileIdentity(path.join(work.out, "bamboo"), MAX_BINARY, 65536), result.receipt.binary);
  await consumer.refreshPublication(state); await consumer.freshConsumer(context, SEAL);
  const line = PREFIX + Buffer.from(core.canonicalJson(completion(state, context, context.current, result.artifact, result))).toString("base64");
  assert.ok(Buffer.byteLength(line) <= MAX_RECEIPT); console.log(line);
}
module.exports = { BUILDER, CONFIG, BUILDER_MANIFEST, REPOSITORY_DIGEST, TARGET, KIND, PREFIX, SELECT, PREPARE, COMPILE, CAPTURE, UPLOAD, SEAL, REUSE, PRODUCTION,
  MAX_ZIP, MAX_BINARY, MAX_RECEIPT, ARGS, artifactName, childEnv, containerArgs, frontend, binding, validateReceipt, history, noOrphan,
  decodeOriginal, completion, readFrame, loadOriginal, readOriginal, manifestHashes, stampInputs, checkBuilder };
if (require.main === module) main().catch(error => { console.error("Docker ARM original: " + error.message); process.exitCode = 1; });
