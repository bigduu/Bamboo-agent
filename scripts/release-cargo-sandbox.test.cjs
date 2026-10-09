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
 let host_write=fs::write(${JSON.stringify(controllerFile)},b"modified").is_ok();
 println!("cargo:warning=ISOLATION own={own} authority={authority} ancestor={seen} host_write={host_write} token={token}");
 assert!(!own && !authority); assert_eq!(seen,${visible}); assert_eq!(host_write,${visible});
 if !${visible} {
  let status=fs::read_to_string("/proc/self/status").unwrap();
  assert!(status.lines().any(|s|s=="NoNewPrivs:\\t1"));
  assert!(status.lines().any(|s|s=="CapEff:\\t0000000000000000"));
  assert!(status.lines().any(|s|s.starts_with("Uid:\\t65532\\t")));
  assert!(!fs::read_to_string("/source/.git/config").unwrap().contains("dummy-checkout-header"));
  if env::var("CARGO_MANIFEST_DIR").unwrap().starts_with("/source/") {fs::write("/source/probe/worker-mutated",b"worker").unwrap();}
 }
}
`;
}
function controller() {
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
  exec("git", ["-C", source, "add", "probe/build.rs"]);
  exec("git", ["-C", source, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-qm", "probe"]);
  const sha = exec("git", ["-C", source, "rev-parse", "HEAD"]).trim();
  // Exercise a linked worktree and dirty stamped manifests, as in publication.
  const linked = path.join(root, "linked");
  exec("git", ["-C", source, "worktree", "add", "--detach", "-q", linked, sha]);
  for (const relative of ["Cargo.toml", "probe/Cargo.toml"]) {
    const file = path.join(linked, relative);
    fs.writeFileSync(file, fs.readFileSync(file, "utf8").replaceAll("0.0.0", "2026.10.17"));
  }
  const staged = "crates/app/bamboo-server/frontend_package";
  fs.mkdirSync(path.join(linked, staged), { recursive: true });
  fs.writeFileSync(path.join(linked, staged, "lotus-frontend.zip"), "dummy-staged-frontend");
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
  assert.match(cargo(["publish", "--dry-run", "--allow-dirty", "--locked", "-p", "fixture-probe"]), /ancestor=false host_write=false token=true/);
  assert.equal(fs.readFileSync(hostFile, "utf8"), "untouched");
  assert.ok(!fs.existsSync(path.join(linked, "probe/worker-mutated")));
  console.log("Isolated metadata/check/package/publish-dry-run and Python shim: passed; exact dirty source provenance: passed");
  fs.rmSync(root, { recursive: true, force: true });
}
if (process.argv[2] === "--controller") controller();
else {
  test("worker client environment excludes publication authority", () => {
    for (const key of ["GH_TOKEN", "GITHUB_TOKEN", "BAMBOO_RELEASE_SIGNING_KEY", "CARGO_REGISTRY_TOKEN", "GITHUB_OUTPUT"]) assert.ok(!(key in sandbox.childEnv()));
    assert.match(sandbox.IMAGE, /^rust:1\.99\.0-bookworm@sha256:[a-f0-9]{64}$/);
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
