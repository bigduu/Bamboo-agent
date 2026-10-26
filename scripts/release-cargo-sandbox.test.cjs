"use strict";
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { spawnSync } = require("node:child_process");
const { test } = require("node:test");
const sandbox = require("./release-cargo-sandbox.cjs");
const helper = path.join(__dirname, "release-cargo-sandbox.cjs");
const temporary = () => fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "bamboo-isolation-test-")));
function exec(bin, args, options = {}) {
  const result = spawnSync(bin, args, { encoding: "utf8", maxBuffer: 32 * 1024 * 1024, ...options });
  assert.equal(result.status, 0, `${bin}: ${result.error || result.stderr || result.stdout}`);
  return result.stdout + (options.logs ? result.stderr : "");
}
function fixture(root) {
  fs.mkdirSync(path.join(root, "src"), { recursive: true });
  fs.mkdirSync(path.join(root, "probe/src"), { recursive: true });
  fs.writeFileSync(path.join(root, "Cargo.toml"), '[workspace]\nmembers=["probe"]\n[package]\nname="bamboo-agent"\nversion="0.0.0"\nedition="2021"\n[dependencies]\nfixture-probe={path="probe",version="0.0.0"}\n');
  fs.writeFileSync(path.join(root, "src/lib.rs"), "pub use fixture_probe::ready;\n");
  fs.writeFileSync(path.join(root, "probe/Cargo.toml"), '[package]\nname="fixture-probe"\nversion="0.0.0"\nedition="2021"\ndescription="Disposable isolation fixture"\nlicense="MIT"\n');
  fs.writeFileSync(path.join(root, "probe/src/lib.rs"), "pub fn ready() {}\n");
  exec("git", ["init", "-q", root]);
  exec("git", ["-C", root, "add", "."]);
  exec("git", ["-C", root, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-qm", "fixture"]);
  exec("git", ["-C", root, "config", "http.https://github.com/.extraheader", "dummy-checkout-header"]);
}
function probe(controllerFile, visible) {
  return `use std::{env,fs};
fn main() {
 println!("cargo:rerun-if-env-changed=CARGO_REGISTRY_TOKEN");
 let mut pid=std::process::id(); let mut seen=false;
 for _ in 0..64 {
  let status=fs::read_to_string(format!("/proc/{pid}/status")).unwrap();
  let parent:u32=status.lines().find_map(|s|s.strip_prefix("PPid:")).unwrap().trim().parse().unwrap();
  if parent==${process.pid} {
   seen=fs::read(format!("/proc/{parent}/environ")).unwrap_or_default().split(|b|*b==0).any(|v|v.starts_with(b"BAMBOO_TEST_PARENT_SECRET=")); break;
  }
  if parent<=1 {break} pid=parent;
 }
 let own=env::var_os("BAMBOO_TEST_PARENT_SECRET").is_some();
 let authority=["GH_TOKEN","GITHUB_TOKEN","GH_ENTERPRISE_TOKEN","GITHUB_ENTERPRISE_TOKEN","BAMBOO_RELEASE_TOKEN","BAMBOO_RELEASE_SIGNING_KEY","BAMBOO_RELEASE_SIGNING_KEY_SHA256","CARGO_REGISTRIES_OTHER_TOKEN","GITHUB_ENV","GITHUB_OUTPUT"].iter().any(|k|env::var_os(k).is_some());
 let token=env::var_os("CARGO_REGISTRY_TOKEN").is_some();
 let host_read=fs::read(${JSON.stringify(controllerFile)}).is_ok();
 let host_write=fs::write(${JSON.stringify(controllerFile)},b"modified").is_ok();
 println!("cargo:warning=ISOLATION own={own} authority={authority} ancestor={seen} host_write={host_write} token={token} host_read={host_read}");
 assert!(!own && !authority); assert_eq!(seen,${visible}); assert_eq!(host_write,${visible});
 assert_eq!(host_read,${visible});
 if !${visible} {
  let status=fs::read_to_string("/proc/self/status").unwrap();
  assert!(status.lines().any(|s|s=="NoNewPrivs:\\t1"));
  assert!(status.lines().any(|s|s=="CapEff:\\t0000000000000000"));
  assert!(status.lines().any(|s|s.starts_with("Uid:\\t65532\\t")));
  assert!(!fs::read_to_string("/source/.git/config").unwrap().contains("dummy-checkout-header"));
  assert_eq!(fs::read("/source/crates/app/bamboo-server/frontend_package/lotus-frontend.zip").unwrap(),b"dummy-staged-frontend");
  assert_eq!(fs::read_to_string("/source/crates/app/bamboo-server/frontend_package/frontend-manifest.json").unwrap(),"{\\\"frontend_version\\\":\\\"dummy-staged\\\"}\\n");
  if env::var("CARGO_MANIFEST_DIR").unwrap().starts_with("/source/") {fs::write("/source/probe/worker-mutated",b"worker").unwrap();}
 }
}
`;
}
async function controller() {
  const root = temporary();
  const hostFile = path.join(root, "controller-only");
  fs.writeFileSync(hostFile, "untouched");
  const source = path.join(root, "checkout");
  fixture(source);
  const clean = { PATH: process.env.PATH, HOME: root, CARGO_HOME: path.join(root, "direct-cargo"), RUSTUP_HOME: process.env.RUSTUP_HOME, RUSTUP_TOOLCHAIN: process.env.RUSTUP_TOOLCHAIN };
  // Only this freshly exec'd controller contains the dummy in its initial env.
  if (process.platform === "linux") {
    fs.writeFileSync(path.join(source, "probe/build.rs"), probe(hostFile, true));
    const output = exec(process.env.BAMBOO_TEST_REAL_CARGO, ["check", "--workspace", "--offline"], { cwd: source, env: clean, logs: true });
    assert.match(output, /own=false authority=false ancestor=true host_write=true token=false/);
    console.log("Linux positive control: ancestor dummy visible; controller write succeeded");
  } else console.log("Local Docker smoke only: Linux ancestor positive control requires Ubuntu CI");
  fs.writeFileSync(hostFile, "untouched");
  fs.writeFileSync(path.join(source, "probe/build.rs"), probe(hostFile, false));
  const staged = "crates/app/bamboo-server/frontend_package";
  fs.mkdirSync(path.join(source, staged), { recursive: true });
  for (const name of ["lotus-frontend.zip", "frontend-manifest.json"]) fs.writeFileSync(path.join(source, staged, name), "stale-committed-frontend");
  exec("git", ["-C", source, "add", "probe/build.rs", staged]);
  exec("git", ["-C", source, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-qm", "probe"]);
  const sha = exec("git", ["-C", source, "rev-parse", "HEAD"]).trim();
  // Exercise a linked worktree and dirty stamped manifests, as in publication.
  const linked = path.join(root, "linked");
  exec("git", ["-C", source, "worktree", "add", "--detach", "-q", linked, sha]);
  for (const relative of ["Cargo.toml", "probe/Cargo.toml"]) {
    const file = path.join(linked, relative);
    fs.writeFileSync(file, fs.readFileSync(file, "utf8").replaceAll("0.0.0", "2026.10.17"));
  }
  fs.writeFileSync(path.join(linked, staged, "lotus-frontend.zip"), "dummy-staged-frontend");
  fs.writeFileSync(path.join(linked, staged, "frontend-manifest.json"), '{"frontend_version":"dummy-staged"}\n');
  const state = sandbox.install(linked, path.join(root, "isolation"));
  const env = { ...process.env, BAMBOO_CARGO_SANDBOX: path.join(state.stateRoot, "state.json"), PATH: path.join(state.stateRoot, "bin") + path.delimiter + process.env.PATH };
  const cargo = args => exec("cargo", args, { cwd: linked, env, logs: args[0] !== "metadata" });
  const metadata = JSON.parse(cargo(["metadata", "--format-version", "1", "--offline"]));
  assert.equal(metadata.target_directory, path.join(linked, "target"));
  assert.equal(metadata.workspace_root, linked);
  assert.ok(metadata.packages.every(pkg => pkg.manifest_path.startsWith(linked + path.sep)));
  assert.ok(fs.statSync(path.join(linked, "Cargo.lock")).isFile());
  assert.match(cargo(["check", "--workspace", "--offline", "--locked"]), /ancestor=false host_write=false token=false/);
  const order = exec("python3", ["-c", 'import subprocess,json; print(len(json.loads(subprocess.check_output(["cargo","metadata","--format-version","1","--no-deps"]))["packages"]))'], { cwd: linked, env });
  assert.equal(order.trim(), "2");
  assert.match(cargo(["package", "--allow-dirty", "--offline", "--locked", "-p", "fixture-probe"]), /ancestor=false host_write=false token=false/);
  const archive = path.join(linked, "target/package/fixture-probe-2026.10.17.crate");
  const vcs = JSON.parse(exec("tar", ["-xOf", archive, "fixture-probe-2026.10.17/.cargo_vcs_info.json"]));
  assert.equal(vcs.git.sha1, sha);
  assert.equal(vcs.git.dirty, true);
  assert.match(cargo(["publish", "--dry-run", "--allow-dirty", "--locked", "-p", "fixture-probe"]), /ancestor=false host_write=false token=false/);
  const reserved = fs.readFileSync(archive);
  fs.appendFileSync(archive, "tampered-reservation");
  const drift = spawnSync("cargo", ["publish", "--dry-run", "--allow-dirty", "--locked", "-p", "fixture-probe"], { cwd: linked, env, encoding: "utf8" });
  assert.notEqual(drift.status, 0);
  assert.match(drift.stderr, /Verified package archive changed from the reserved bytes/);
  assert.deepEqual(fs.readFileSync(archive), reserved);
  // Exercise the real non-dry publication route, replacing only its final
  // network call. No production endpoint or credential is used by this test.
  const uploader = require("./release-cargo-upload.cjs");
  const upload = uploader.uploadArchive;
  let uploads = 0;
  uploader.uploadArchive = async (bytes, metadata, token) => {
    uploads++;
    assert.equal(token, "dummy-registry");
    assert.equal(metadata.name, "fixture-probe");
    assert.equal(metadata.vers, "2026.10.17");
    assert.deepEqual(bytes, reserved);
    return { status: 0, output: "Fixture upload of exact verified bytes" };
  };
  try {
    const buildFile = path.join(linked, "probe/build.rs");
    const build = fs.readFileSync(buildFile);
    fs.writeFileSync(buildFile, 'compile_error!("fixture verification must stop upload");\n');
    assert.notEqual(await sandbox.run(state, ["publish", "--locked", "--allow-dirty", "-p", "fixture-probe"]), 0);
    assert.equal(uploads, 0);
    fs.writeFileSync(buildFile, build);
    assert.equal(await sandbox.run(state, ["publish", "--locked", "--allow-dirty", "-p", "fixture-probe"]), 0);
    assert.equal(uploads, 1);
  } finally { uploader.uploadArchive = upload; }
  assert.equal(fs.readFileSync(hostFile, "utf8"), "untouched");
  assert.ok(!fs.existsSync(path.join(linked, "probe/worker-mutated")));
  console.log("Isolated metadata/check/package/publish-dry-run and Python shim: passed; exact dirty source provenance: passed");
  fs.rmSync(root, { recursive: true, force: true });
}
if (process.argv[2] === "--controller") controller().catch(error => { console.error(error); process.exitCode = 1; });
else {
  test("worker client environment excludes publication authority", () => {
    for (const key of ["GH_TOKEN", "GITHUB_TOKEN", "BAMBOO_RELEASE_SIGNING_KEY", "CARGO_REGISTRY_TOKEN", "GITHUB_OUTPUT"]) assert.ok(!(key in sandbox.childEnv()));
    assert.match(sandbox.IMAGE, /^rust:1\.99\.0-bookworm@sha256:[a-f0-9]{64}$/);
  });
  test("publication rejects credential, registry and execution overrides before starting a worker", async () => {
    for (const flag of ["--token", "--registry", "--index", "--config", "--no-verify", "--features"]) {
      await assert.rejects(sandbox.publishArchive({}, ["publish", "--locked", "--allow-dirty", "-p", "fixture-probe", flag]), /Unsupported publication argument/);
    }
    await assert.rejects(sandbox.publishArchive({}, ["publish", "--locked", "--allow-dirty", "--dry-run", "-p", "fixture-probe", "-p", "other"]), /one explicit package/);
  });
  test("transfers reject source and destination symlinks, including parent directories", () => {
    const root = temporary();
    fs.mkdirSync(path.join(root, "from")); fs.mkdirSync(path.join(root, "to"));
    fs.writeFileSync(path.join(root, "from/lock"), "fixed");
    fs.symlinkSync(path.join(root, "from/lock"), path.join(root, "from/link"));
    assert.throws(() => sandbox.copyRegular(path.join(root, "from"), "link", path.join(root, "to")), /Symlink/);
    fs.symlinkSync(path.join(root, "from"), path.join(root, "to/parent"));
    assert.throws(() => sandbox.copyRegular(root, "from/lock", path.join(root, "to/parent")), /Unsafe/);
    fs.symlinkSync(path.join(root, "from/lock"), path.join(root, "to/lock"));
    assert.throws(() => sandbox.copyRegular(path.join(root, "from"), "lock", path.join(root, "to")), /Symlink/);
    fs.rmSync(root, { recursive: true, force: true });
  });
  test("snapshot preserves dirty files and source SHA without host Git configuration", () => {
    const root = temporary(); fixture(path.join(root, "repo"));
    const sha = exec("git", ["-C", path.join(root, "repo"), "rev-parse", "HEAD"]).trim();
    fs.writeFileSync(path.join(root, "repo/src/lib.rs"), "// stamped\n");
    sandbox.snapshot(path.join(root, "repo"), path.join(root, "copy"), sha);
    assert.equal(exec("git", ["-C", path.join(root, "copy"), "rev-parse", "HEAD"]).trim(), sha);
    assert.equal(fs.readFileSync(path.join(root, "copy/src/lib.rs"), "utf8"), "// stamped\n");
    assert.ok(!fs.readFileSync(path.join(root, "copy/.git/config"), "utf8").includes("dummy-checkout-header"));
    assert.ok(!fs.existsSync(path.join(root, "copy/.git/objects/info/alternates")));
    fs.rmSync(root, { recursive: true, force: true });
  });
  test("Docker failure is fatal with no host Cargo fallback", () => {
    const root = temporary(); fixture(path.join(root, "repo"));
    const state = sandbox.install(path.join(root, "repo"), path.join(root, "state"));
    fs.mkdirSync(path.join(root, "bin"));
    fs.writeFileSync(path.join(root, "bin/docker"), "#!/bin/sh\nexit 79\n", { mode: 0o755 });
    const result = spawnSync(process.execPath, [helper, "metadata", "--format-version", "1"], { env: { PATH: path.join(root, "bin") + path.delimiter + process.env.PATH, BAMBOO_CARGO_SANDBOX: path.join(state.stateRoot, "state.json") } });
    assert.equal(result.status, 79);
    assert.ok(!fs.existsSync(path.join(state.source, "Cargo.lock")));
    fs.rmSync(root, { recursive: true, force: true });
  });
  test("untrusted output stays between command markers on one pipe; transfers replace regular files atomically", () => {
    const root = temporary(); fixture(path.join(root, "repo"));
    const state = sandbox.install(path.join(root, "repo"), path.join(root, "state"));
    fs.mkdirSync(path.join(root, "bin"));
    fs.writeFileSync(path.join(root, "bin/docker"), "#!/bin/sh\necho '::warning::untrusted-stdout'\necho '::warning::untrusted-stderr' >&2\n", { mode: 0o755 });
    const result = spawnSync(process.execPath, [helper, "--version"], { encoding: "utf8", env: { GITHUB_ACTIONS: "true", PATH: path.join(root, "bin") + path.delimiter + process.env.PATH, BAMBOO_CARGO_SANDBOX: path.join(state.stateRoot, "state.json") } });
    assert.equal(result.status, 0); assert.equal(result.stdout, "");
    assert.match(result.stderr, /^::stop-commands::([a-f0-9]{48})\n::warning::untrusted-stderr\n::warning::untrusted-stdout\n\n::\1::\n$/);
    fs.mkdirSync(path.join(root, "output")); fs.writeFileSync(path.join(root, "repo/Cargo.lock"), "new-lock"); fs.writeFileSync(path.join(root, "output/Cargo.lock"), "old-lock");
    sandbox.copyRegular(path.join(root, "repo"), "Cargo.lock", path.join(root, "output"), true);
    assert.equal(fs.readFileSync(path.join(root, "output/Cargo.lock"), "utf8"), "new-lock");
    assert.deepEqual(fs.readdirSync(path.join(root, "output")), ["Cargo.lock"]);
    fs.rmSync(root, { recursive: true, force: true });
  });
  test("disk policy is bounded and cannot select a deletion path", () => {
    assert.deepEqual(sandbox.diskPolicy(), { lowFreeBytes: 2 * 1024 ** 3, maxScratchBytes: 8 * 1024 ** 3 });
    assert.deepEqual(sandbox.diskPolicy({ lowFreeBytes: 64 * 1024 ** 2, maxScratchBytes: 96 * 1024 ** 2 }), { lowFreeBytes: 64 * 1024 ** 2, maxScratchBytes: 96 * 1024 ** 2 });
    for (const policy of [{ lowFreeBytes: 1, maxScratchBytes: 32 * 1024 ** 2 }, { lowFreeBytes: 64 * 1024 ** 2, maxScratchBytes: 32 * 1024 ** 2 }, { lowFreeBytes: 32 * 1024 ** 2, maxScratchBytes: 32 * 1024 ** 3 }, { ...sandbox.DEFAULT_DISK_POLICY, path: "/outside" }]) assert.throws(() => sandbox.diskPolicy(policy));
  });
  test("ENOSPC classification requires a completed Cargo failure and an explicit space diagnostic", () => {
    const failed = { status: 101, signal: null, stderr: "error: No space left on device", stdout: "" };
    assert.equal(sandbox.enospc(failed), true);
    assert.equal(sandbox.enospc({ ...failed, stderr: "", stdout: "error: ENOSPC" }), true);
    for (const change of [
      { status: 79 }, { status: 125 }, { status: 126 }, { status: 127 }, { status: null },
      { signal: "SIGTERM" }, { error: Object.assign(new Error("ENOSPC"), { code: "ENOSPC" }) },
      { stderr: "error: ENOBUFS" }, { stderr: "error: Permission denied (EACCES)" },
      { stderr: "error: failed to compile" }, { stderr: "Error response from daemon: ENOSPC" },
    ]) assert.equal(sandbox.enospc({ ...failed, ...change }), false);
  });
  test("non-package commands do not gain an ENOSPC retry or scratch reclamation", t => {
    const root = temporary(); t.after(() => fs.rmSync(root, { recursive: true, force: true }));
    const source = path.join(root, "repo"); fixture(source);
    const state = sandbox.install(source, path.join(root, "state"));
    const debug = path.join(state.stateRoot, "data/target/debug"); fs.mkdirSync(debug);
    fs.writeFileSync(path.join(debug, "keep"), "compiled");
    const count = path.join(root, "calls"); const bin = path.join(root, "bin"); fs.mkdirSync(bin);
    fs.writeFileSync(path.join(bin, "docker"), '#!/usr/bin/env node\n' +
      `require("node:fs").appendFileSync(${JSON.stringify(count)}, "call\\n"); console.error("error: ENOSPC"); process.exit(101);\n`, { mode: 0o755 });
    for (const command of ["metadata", "check", "build", "test"]) {
      const result = spawnSync(process.execPath, [helper, command], { encoding: "utf8", env: {
        PATH: bin + path.delimiter + process.env.PATH, HOME: process.env.HOME,
        BAMBOO_CARGO_SANDBOX: path.join(state.stateRoot, "state.json"),
      } });
      assert.equal(result.status, 101, result.stderr);
      assert.doesNotMatch(result.stderr, /\[cargo-disk\]/);
      assert.equal(fs.readFileSync(path.join(debug, "keep"), "utf8"), "compiled");
    }
    assert.equal(fs.readFileSync(count, "utf8"), "call\n".repeat(4));
  });
  function diskFixture(root) {
    const stateRoot = path.join(root, "state");
    fs.mkdirSync(stateRoot, { mode: 0o700 });
    for (const part of ["data/target/debug", "data/target/package", "data/cache", "source/target/package"]) fs.mkdirSync(path.join(stateRoot, part), { recursive: true });
    for (const file of ["data/target/debug/compiler", "data/target/package/reserved.crate", "data/cache/registry", "source/target/package/reserved.crate", "source/manifest", "receipt.json"]) fs.writeFileSync(path.join(stateRoot, file), file);
    return { stateRoot, diskPolicy: { lowFreeBytes: 16 * 1024 ** 2, maxScratchBytes: 32 * 1024 ** 2 } };
  }
  test("low free space reclaims only owned debug and preserves archives, cache, source and receipts", t => {
    const root = temporary(); t.after(() => fs.rmSync(root, { recursive: true, force: true }));
    const state = diskFixture(root);
    const keep = ["data/target/package/reserved.crate", "data/cache/registry", "source/target/package/reserved.crate", "source/manifest", "receipt.json"];
    t.mock.method(fs, "statfsSync", () => ({ bsize: 1, bavail: 1 }));
    let calls = 0;
    const result = sandbox.withDiskRecovery(state, ["package", "-p", "fixture-probe"], () => {
      calls++; assert.ok(!fs.existsSync(path.join(state.stateRoot, "data/target/debug")));
      return { status: 0, stdout: "", stderr: "", signal: null };
    });
    assert.equal(result.status, 0); assert.equal(calls, 1);
    for (const file of keep) assert.equal(fs.readFileSync(path.join(state.stateRoot, file), "utf8"), file);
  });
  test("adequate space retains compiler scratch between packages and sparse files do not exhaust its budget", t => {
    const root = temporary(); t.after(() => fs.rmSync(root, { recursive: true, force: true }));
    const state = diskFixture(root);
    t.mock.method(fs, "statfsSync", () => ({ bsize: 1, bavail: 1024 ** 3 }));
    const sparse = path.join(state.stateRoot, "data/target/debug/sparse");
    const fd = fs.openSync(sparse, "w"); fs.ftruncateSync(fd, 128 * 1024 ** 2); fs.closeSync(fd);
    let calls = 0;
    for (let packageIndex = 0; packageIndex < 2; packageIndex++) sandbox.withDiskRecovery(state, ["package", "-p", "fixture-probe"], () => {
      calls++; assert.ok(fs.existsSync(sparse)); assert.ok(fs.existsSync(path.join(state.stateRoot, "data/target/debug/compiler")));
      return { status: 0, stdout: "", stderr: "", signal: null };
    });
    assert.equal(calls, 2);
  });
  test("scratch reclamation rejects linked roots or parents and never follows nested links", t => {
    const root = temporary(); const outside = temporary();
    t.after(() => { fs.rmSync(root, { recursive: true, force: true }); fs.rmSync(outside, { recursive: true, force: true }); });
    const state = diskFixture(root); const marker = path.join(outside, "keep"); fs.writeFileSync(marker, "outside");
    const debug = path.join(state.stateRoot, "data/target/debug");
    fs.symlinkSync(outside, path.join(debug, "nested-link"));
    sandbox.reclaimScratch(state); assert.equal(fs.readFileSync(marker, "utf8"), "outside");
    fs.symlinkSync(outside, debug);
    assert.throws(() => sandbox.reclaimScratch(state), /Unsafe compiler scratch/);
    fs.unlinkSync(debug); fs.rmSync(path.join(state.stateRoot, "data/target"), { recursive: true });
    fs.symlinkSync(outside, path.join(state.stateRoot, "data/target"));
    assert.throws(() => sandbox.reclaimScratch(state), /Unsafe compiler scratch parent/);
    assert.equal(fs.readFileSync(marker, "utf8"), "outside");
  });
  test("fake Docker retries only one normal Cargo ENOSPC with identical verified-package arguments", async t => {
    const matrix = [
      { name: "ENOSPC once succeeds", results: [{ status: 101, stderr: "error: No space left on device (ENOSPC)\n" }, { status: 0, stderr: "verified\n" }], calls: 2, status: 0 },
      { name: "normal compiler failure", results: [{ status: 101, stderr: "error: unresolved import\n" }], calls: 1, status: 101 },
      { name: "second ENOSPC stays fatal", results: [{ status: 101, stderr: "error: ENOSPC first\n" }, { status: 101, stderr: "error: ENOSPC second\n" }], calls: 2, status: 101 },
      { name: "Docker reserved failure", results: [{ status: 125, stderr: "docker: ENOSPC\n", unsafe: true }], calls: 1, status: 125 },
      { name: "ambiguous daemon error", results: [{ status: 101, stderr: "Error response from daemon: ENOSPC\n", unsafe: true }], calls: 1, status: 101 },
      { name: "Docker signal", results: [{ signal: "SIGTERM", stderr: "ENOSPC\n", unsafe: true }], calls: 1, status: 1 },
    ];
    for (const scenario of matrix) await t.test(scenario.name, () => {
      const root = temporary(); const outside = temporary();
      try {
        const source = path.join(root, "repo"); fixture(source);
        const state = sandbox.install(source, path.join(root, "state"), { lowFreeBytes: 16 * 1024 ** 2, maxScratchBytes: 32 * 1024 ** 2 });
        const target = path.join(state.stateRoot, "data/target"); const debug = path.join(target, "debug");
        fs.mkdirSync(debug); fs.writeFileSync(path.join(debug, "original"), "compiled");
        const reserved = Buffer.from("fixed reserved archive"); const archive = "fixture-probe-0.0.0.crate";
        for (const base of [path.join(target, "package"), path.join(source, "target/package")]) { fs.mkdirSync(base, { recursive: true }); fs.writeFileSync(path.join(base, archive), reserved); }
        fs.writeFileSync(path.join(outside, "keep"), "outside");
        const receipt = fs.readFileSync(path.join(state.stateRoot, "state.json"));
        const counter = path.join(state.stateRoot, "data/cache/calls.json");
        const bin = path.join(root, "bin"); fs.mkdirSync(bin);
        fs.writeFileSync(path.join(bin, "docker"), "#!/usr/bin/env node\n" + `
const fs=require('node:fs');
const counter=${JSON.stringify(counter)},debug=${JSON.stringify(debug)},outside=${JSON.stringify(outside)};
const calls=fs.existsSync(counter)?JSON.parse(fs.readFileSync(counter)):[];
calls.push({args:process.argv.slice(2),hadScratch:fs.existsSync(debug)});fs.writeFileSync(counter,JSON.stringify(calls));
if(process.env.CARGO_REGISTRY_TOKEN||process.env.GH_TOKEN||process.env.BAMBOO_RELEASE_SIGNING_KEY)process.exit(88);
const results=${JSON.stringify(scenario.results)};const result=results[Math.min(calls.length-1,results.length-1)];
fs.mkdirSync(debug,{recursive:true});fs.writeFileSync(debug+'/attempt','compiled');
if(result.unsafe){fs.rmSync(debug,{recursive:true});fs.symlinkSync(outside,debug);}
process.stderr.write(result.stderr);if(result.signal)process.kill(process.pid,result.signal);else process.exit(result.status);
`, { mode: 0o755 });
        const args = ["package", "--allow-dirty", "--locked", "-p", "fixture-probe"];
        const result = spawnSync(process.execPath, [helper, ...args], { encoding: "utf8", env: { PATH: bin + path.delimiter + process.env.PATH, HOME: process.env.HOME, DOCKER_HOST: state.endpoint, BAMBOO_CARGO_SANDBOX: path.join(state.stateRoot, "state.json"), CARGO_REGISTRY_TOKEN: "dummy-parent-only", GH_TOKEN: "dummy-parent-only", BAMBOO_RELEASE_SIGNING_KEY: "dummy-parent-only" } });
        assert.equal(result.status, scenario.status, result.stderr);
        const calls = JSON.parse(fs.readFileSync(counter)); assert.equal(calls.length, scenario.calls);
        assert.ok(calls.every(call => assert.deepEqual(call.args.slice(-args.length), args) === undefined));
        if (calls.length === 2) assert.equal(calls[1].hadScratch, false);
        assert.equal((result.stderr.match(/^\[cargo-disk\] phase=enospc-retry attempt=2 /gm) || []).length, scenario.calls - 1);
        if (scenario.results[0].unsafe) { assert.ok(fs.lstatSync(debug).isSymbolicLink()); assert.ok(!result.stderr.includes("phase=package-end")); }
        else assert.equal((result.stderr.match(/phase=package-end/g) || []).length, scenario.calls);
        assert.deepEqual(fs.readFileSync(path.join(target, "package", archive)), reserved);
        assert.deepEqual(fs.readFileSync(path.join(source, "target/package", archive)), reserved);
        assert.deepEqual(fs.readFileSync(path.join(state.stateRoot, "state.json")), receipt);
        assert.equal(fs.readFileSync(path.join(source, "src/lib.rs"), "utf8"), "pub use fixture_probe::ready;\n");
        assert.equal(fs.readFileSync(path.join(outside, "keep"), "utf8"), "outside");
        if (scenario.calls === 2 && scenario.status !== 0) assert.ok(fs.existsSync(path.join(debug, "attempt")), "second ENOSPC does not clean or retry again");
      } finally { fs.rmSync(root, { recursive: true, force: true }); fs.rmSync(outside, { recursive: true, force: true }); }
    });
  });
  test("worker permission repair ignores symlinks, permits generated directory cleanup and retains Cargo status", { skip: !process.getuid }, () => {
    const root = temporary();
    try {
      for (const part of ["source", "cache", "target", "outside", "bin"]) fs.mkdirSync(path.join(root, part));
      const outside = path.join(root, "outside/keep"); fs.writeFileSync(outside, "outside", { mode: 0o400 });
      fs.writeFileSync(path.join(root, "bin/cargo"), '#!/bin/sh\nmkdir -p "$TEST_SOURCE/generated/nested"\nln -s "$TEST_OUTSIDE" "$TEST_SOURCE/generated/link"\nchmod 000 "$TEST_SOURCE/generated/nested"\nexit 73\n', { mode: 0o755 });
      const quote = value => "'" + value.replaceAll("'", "'\\''") + "'";
      const script = sandbox.WORKER_SCRIPT.replaceAll("/source", quote(path.join(root, "source"))).replaceAll("/cargo-home", quote(path.join(root, "cache"))).replaceAll("/target", quote(path.join(root, "target"))).replaceAll("-uid 65532", `-uid ${process.getuid()}`);
      const result = spawnSync("/bin/sh", ["-c", script, "cargo", "--version"], { encoding: "utf8", env: { PATH: path.join(root, "bin") + path.delimiter + process.env.PATH, TEST_SOURCE: path.join(root, "source"), TEST_OUTSIDE: path.join(root, "outside") } });
      assert.equal(result.status, 73); assert.equal(result.stderr, "");
      assert.equal(fs.statSync(outside).mode & 0o777, 0o400);
      assert.doesNotThrow(() => fs.rmSync(path.join(root, "source/generated"), { recursive: true }));
      assert.match(sandbox.WORKER_SCRIPT, /find -P .* -uid 65532/); assert.ok(!sandbox.WORKER_SCRIPT.includes("chmod -R"));
    } finally { fs.rmSync(root, { recursive: true, force: true }); }
  });
  test("real Cargo isolation with dummy controller authority", { skip: process.env.BAMBOO_CARGO_INTEGRATION !== "1" }, () => {
    const env = { PATH: process.env.PATH, HOME: process.env.HOME, DOCKER_HOST: process.env.DOCKER_HOST, DOCKER_CONTEXT: process.env.DOCKER_CONTEXT,
      BAMBOO_TEST_REAL_CARGO: exec("sh", ["-c", "command -v cargo"]).trim(), RUSTUP_HOME: process.env.RUSTUP_HOME || path.join(os.homedir(), ".rustup"), RUSTUP_TOOLCHAIN: "stable",
      BAMBOO_TEST_PARENT_SECRET: "dummy-parent-only", GH_TOKEN: "dummy-github", GITHUB_TOKEN: "dummy-github", GH_ENTERPRISE_TOKEN: "dummy-enterprise", GITHUB_ENTERPRISE_TOKEN: "dummy-enterprise", BAMBOO_RELEASE_TOKEN: "dummy-release", BAMBOO_RELEASE_SIGNING_KEY: "dummy-hmac", BAMBOO_RELEASE_SIGNING_KEY_SHA256: "dummy-fingerprint", CARGO_REGISTRY_TOKEN: "dummy-registry", CARGO_REGISTRIES_OTHER_TOKEN: "dummy-other-registry", GITHUB_ENV: "dummy-runner-env", GITHUB_OUTPUT: "dummy-runner-output" };
    const output = exec(process.execPath, [__filename, "--controller"], { env, logs: true });
    assert.match(output, /Isolated metadata\/check\/package\/publish-dry-run/);
    if (process.platform === "linux") assert.match(output, /Linux positive control: ancestor dummy visible/);
    console.log(output.trim());
  });
}
