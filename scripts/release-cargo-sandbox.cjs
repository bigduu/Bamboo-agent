#!/usr/bin/env node
"use strict";

// This controller is never mounted into a Cargo worker. Each invocation gets a
// disposable source tree; only inert lock/package bytes can return to the host.
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const crypto = require("node:crypto");
const { spawnSync } = require("node:child_process");
const assert = require("node:assert/strict");
const IMAGE = "rust:1.99.0-bookworm@sha256:114c7a4425406451c2866b6aafe69fe29b1b298832db1277d411ac73c82d04d6";
const FRONTEND = "crates/app/bamboo-server/frontend_package";

function childEnv() {
  return Object.fromEntries(["PATH", "HOME"]
    .filter(key => process.env[key]).map(key => [key, process.env[key]]));
}
function command(bin, args, options = {}) {
  const result = spawnSync(bin, args, { env: childEnv(), encoding: "utf8", maxBuffer: 64 * 1024 * 1024, ...options });
  if (result.error) throw result.error;
  assert.equal(result.status, 0, `${bin} failed: ${result.stderr}`);
  return result.stdout;
}
function git(root, args) {
  return command("git", ["-c", "core.hooksPath=/dev/null", "-C", root, ...args], {
    env: { PATH: process.env.PATH, GIT_CONFIG_NOSYSTEM: "1", GIT_CONFIG_GLOBAL: "/dev/null" },
  });
}
function regular(root, relative) {
  assert.ok(!path.isAbsolute(relative) && !relative.split(path.sep).includes(".."), "Unsafe relative path");
  let current = root;
  assert.ok(fs.lstatSync(current).isDirectory() && !fs.lstatSync(current).isSymbolicLink(), "Unsafe output root");
  for (const part of relative.split(path.sep)) {
    current = path.join(current, part);
    const stat = fs.lstatSync(current);
    assert.ok(!stat.isSymbolicLink(), "Symlink in sandbox transfer");
    assert.ok(current === path.join(root, relative) ? stat.isFile() : stat.isDirectory(), "Non-regular sandbox transfer");
  }
  return current;
}
function directories(root, relative) {
  let current = root;
  assert.ok(fs.lstatSync(root).isDirectory() && !fs.lstatSync(root).isSymbolicLink(), "Unsafe destination root");
  for (const part of relative.split(path.sep).filter(Boolean)) {
    assert.ok(part !== ".." && part !== ".git", "Unsafe destination");
    current = path.join(current, part);
    if (!fs.existsSync(current)) fs.mkdirSync(current);
    assert.ok(fs.lstatSync(current).isDirectory() && !fs.lstatSync(current).isSymbolicLink(), "Unsafe destination parent");
  }
  return current;
}
function copyRegular(from, relative, to, atomic = false) {
  const input = regular(from, relative);
  directories(to, path.dirname(relative) === "." ? "" : path.dirname(relative));
  const output = path.join(to, relative);
  if (fs.existsSync(output) || fs.lstatSync(output, { throwIfNoEntry: false })) regular(to, relative);
  const temporary = atomic ? path.join(path.dirname(output), `.bamboo-transfer-${crypto.randomBytes(12).toString("hex")}`) : output;
  try {
    fs.copyFileSync(input, temporary, atomic ? fs.constants.COPYFILE_EXCL : 0);
    fs.chmodSync(temporary, fs.statSync(input).mode & 0o111 ? 0o777 : 0o666);
    fs.utimesSync(temporary, fs.statSync(input).atime, fs.statSync(input).mtime);
    if (atomic) fs.renameSync(temporary, output);
  } finally { if (atomic) fs.rmSync(temporary, { force: true }); }
}
function snapshot(source, destination, revision) {
  fs.mkdirSync(destination);
  git(destination, ["init", "--quiet"]);
  // Fetch creates independent objects, including for linked worktrees. It never
  // copies config, hooks, alternates or a pointer to the host common Git dir.
  git(destination, ["fetch", "--quiet", "--no-tags", "--depth=1", source, revision]);
  git(destination, ["checkout", "--quiet", "--detach", revision]);
  for (const name of ["hooks", "logs", "FETCH_HEAD"]) fs.rmSync(path.join(destination, ".git", name), { recursive: true, force: true });
  fs.writeFileSync(path.join(destination, ".git/config"), "[core]\nrepositoryformatversion = 0\nbare = false\nfilemode = true\n");
  for (const relative of git(source, ["ls-files", "-z"]).split("\0").filter(Boolean)) {
    assert.ok(!relative.split("/").includes(".git"), "Unexpected Git metadata path");
    copyRegular(source, relative, destination);
  }
  // These may be ignored by Git, but publication must use the staged frontend.
  for (const name of ["Cargo.lock", `${FRONTEND}/frontend-manifest.json`, `${FRONTEND}/lotus-frontend.zip`]) {
    if (fs.existsSync(path.join(source, name))) copyRegular(source, name, destination);
  }
  function writable(directory) {
    fs.chmodSync(directory, 0o777);
    for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
      const file = path.join(directory, entry.name);
      assert.ok(!entry.isSymbolicLink(), "Source symlinks are not supported by publication");
      if (entry.isDirectory()) writable(file);
      else fs.chmodSync(file, fs.statSync(file).mode & 0o111 ? 0o777 : 0o666);
    }
  }
  writable(destination);
}
function install(source, stateRoot = fs.mkdtempSync(path.join(os.tmpdir(), "bamboo-cargo-"))) {
  source = fs.realpathSync(source);
  const revision = git(source, ["rev-parse", "HEAD"]).trim();
  assert.match(revision, /^[0-9a-f]{40}$/);
  fs.mkdirSync(stateRoot, { recursive: true, mode: 0o700 });
  fs.chmodSync(stateRoot, 0o700);
  for (const name of ["bin", "data", "data/cache", "data/target"]) {
    fs.mkdirSync(path.join(stateRoot, name), { recursive: true, mode: 0o777 });
    fs.chmodSync(path.join(stateRoot, name), 0o777);
  }
  fs.chmodSync(path.join(stateRoot, "bin"), 0o700);
  fs.mkdirSync(path.join(stateRoot, "docker"), { mode: 0o700 });
  const endpoint = process.env.DOCKER_HOST || command("docker", ["context", "inspect", ...(process.env.DOCKER_CONTEXT ? [process.env.DOCKER_CONTEXT] : []), "--format", '{{(index .Endpoints "docker").Host}}']).trim();
  assert.match(endpoint, /^unix:\/\/\//, "Publication requires a local Unix Docker endpoint");
  const state = { source, revision, stateRoot, endpoint, target: path.join(source, "target") };
  fs.writeFileSync(path.join(stateRoot, "state.json"), JSON.stringify(state), { mode: 0o600 });
  fs.copyFileSync(__filename, path.join(stateRoot, "bin/cargo"));
  copyRegular(__dirname, "release-cargo-upload.cjs", path.join(stateRoot, "bin"));
  fs.chmodSync(path.join(stateRoot, "bin/cargo"), 0o700);
  return state;
}
function remapMetadata(output, state) {
  const data = JSON.parse(output);
  const mapped = value => typeof value === "string" && (value === "/source" || value.startsWith("/source/"))
    ? state.source + value.slice(7) : value;
  data.workspace_root = mapped(data.workspace_root);
  data.target_directory = state.target;
  if (data.build_directory) data.build_directory = state.target;
  for (const pkg of data.packages) {
    pkg.manifest_path = mapped(pkg.manifest_path);
    for (const target of pkg.targets) target.src_path = mapped(target.src_path);
    for (const dep of pkg.dependencies) if (dep.path) dep.path = mapped(dep.path);
  }
  return JSON.stringify(data) + "\n";
}
function packageFile(state, args) {
  if (!["package", "publish"].includes(args[0]) || args.includes("--list")) return null;
  const index = args.findIndex(arg => arg === "-p" || arg === "--package");
  assert.ok(index >= 0 && args.filter(arg => arg === "-p" || arg === "--package").length === 1 && /^[A-Za-z0-9_-]+$/.test(args[index + 1]), "Package output requires one explicit -p name");
  const manifests = git(state.source, ["ls-files", "-z"]).split("\0").filter(file => file === "Cargo.toml" || file.endsWith("/Cargo.toml"));
  for (const file of manifests) regular(state.source, file);
  const file = command("python3", ["-I", "-c", `import json,sys,tomllib
from pathlib import Path
root=Path(sys.argv[1])
documents=[tomllib.loads((root/p).read_text()) for p in json.loads(sys.argv[3])]
packages=[d['package'] for d in documents if d.get('package',{}).get('name')==sys.argv[2]]
assert len(packages)==1
version=packages[0]['version']
if isinstance(version,dict): version=tomllib.loads((root/'Cargo.toml').read_text())['workspace']['package']['version']
print(sys.argv[2]+'-'+version+'.crate')`, state.source, args[index + 1], JSON.stringify(manifests)]).trim();
  assert.match(file, /^[A-Za-z0-9_-]+-[0-9][A-Za-z0-9.+-]*\.crate$/);
  return file;
}
function recover(state, source, file) {
  if (fs.lstatSync(path.join(source, "Cargo.lock"), { throwIfNoEntry: false })) copyRegular(source, "Cargo.lock", state.source, true);
  if (!file) return;
  const packageRoot = path.join(state.stateRoot, "data/target");
  const destination = directories(state.source, "target/package");
  assert.equal(destination, path.join(state.target, "package"));
  copyRegular(packageRoot, `package/${file}`, state.target, true);
}
async function publishArchive(state, args) {
  // Package verification may execute arbitrary build scripts and Cargo config.
  // It must run in the same credential-free worker as every other Cargo command.
  const flags = new Set();
  let name;
  for (let index = 1; index < args.length; index++) {
    const arg = args[index];
    if (arg === "-p" || arg === "--package") {
      assert.equal(name, undefined, "Publication requires one explicit package");
      name = args[++index];
      assert.match(name || "", /^[A-Za-z0-9_-]+$/);
    } else {
      assert.ok(["--locked", "--allow-dirty", "--dry-run"].includes(arg) && !flags.has(arg), "Unsupported publication argument");
      flags.add(arg);
    }
  }
  assert.ok(name && flags.has("--locked") && flags.has("--allow-dirty"), "Publication requires --locked --allow-dirty and one -p package");
  const dryRun = flags.has("--dry-run");
  assert.ok(dryRun || process.env.CARGO_REGISTRY_TOKEN, "Missing CARGO_REGISTRY_TOKEN");
  const file = packageFile(state, args);
  const archivePath = path.join(state.target, "package", file);
  const sha256 = bytes => crypto.createHash("sha256").update(bytes).digest("hex");
  // Automatic publication reserves this checksum before calling the shim. A
  // second package verification must never substitute other bytes for it.
  const expected = fs.lstatSync(archivePath, { throwIfNoEntry: false })
    ? sha256(fs.readFileSync(regular(state.target, `package/${file}`))) : null;
  const status = run(state, ["package", "--locked", "--allow-dirty", "-p", name]);
  if (status !== 0) return status;
  const bytes = fs.readFileSync(regular(state.target, `package/${file}`));
  if (expected) assert.equal(sha256(bytes), expected, "Verified package archive changed from the reserved bytes");
  const { describeArchive, uploadArchive } = require("./release-cargo-upload.cjs");
  const version = file.slice(name.length + 1, -".crate".length);
  const metadata = describeArchive(bytes, name, version);
  if (dryRun) {
    process.stderr.write(`Verified ${name}@${version} without credentials; upload skipped for dry run\n`);
    return 0;
  }
  // This trusted HTTP client consumes the already-read immutable bytes. It runs
  // no Cargo, build script, dependency, source hook or credential provider.
  const result = await uploadArchive(bytes, metadata, process.env.CARGO_REGISTRY_TOKEN);
  process.stderr.write(result.output + "\n");
  return result.status;
}
function run(state, args) {
  assert.ok(["metadata", "check", "build", "test", "package", "publish", "--version"].includes(args[0]), "Unsupported publication Cargo command");
  assert.equal(git(state.source, ["rev-parse", "HEAD"]).trim(), state.revision, "Publication source moved");
  if (args[0] === "publish") return publishArchive(state, args);
  const outputFile = packageFile(state, args);
  const work = fs.mkdtempSync(path.join(state.stateRoot, "data/work-"));
  const source = path.join(work, "source");
  const env = { ...childEnv(), DOCKER_HOST: state.endpoint };
  try {
    snapshot(state.source, source, state.revision);
    const dockerArgs = ["--config", path.join(state.stateRoot, "docker"), "run", "--rm", "--init", "--user", "65532:65532", "--cap-drop=ALL", "--security-opt=no-new-privileges", "--read-only",
      "--tmpfs", "/tmp:rw,nosuid,nodev,mode=1777", "--mount", `type=bind,src=${source},dst=/source`,
      "--mount", `type=bind,src=${path.join(state.stateRoot, "data/cache")},dst=/cargo-home`,
      "--mount", `type=bind,src=${path.join(state.stateRoot, "data/target")},dst=/target`, "--workdir", "/source",
      "--env", "HOME=/tmp", "--env", "CARGO_HOME=/cargo-home", "--env", "CARGO_TARGET_DIR=/target",
      "--env", "RUSTUP_TOOLCHAIN=1.99.0", "--env", "RUSTUP_AUTO_INSTALL=0", "--env", "CARGO_TERM_COLOR=never"];
    dockerArgs.push(IMAGE, "/bin/sh", "-c", 'umask 022; cargo "$@"; status=$?; chmod -R a+rwX /source /cargo-home /target; exit "$status"', "cargo", ...args);
    const result = spawnSync("docker", dockerArgs, { env, encoding: "utf8", maxBuffer: 64 * 1024 * 1024 });
    if (result.error) throw result.error;
    // Build output is untrusted; it must not issue runner workflow commands.
    const stop = crypto.randomBytes(24).toString("hex");
    if (process.env.GITHUB_ACTIONS) process.stderr.write(`::stop-commands::${stop}\n`);
    process.stderr.write(result.stderr || "");
    if (result.status === 0 && args[0] === "metadata") process.stdout.write(remapMetadata(result.stdout, state));
    else process.stderr.write(result.stdout || "");
    if (process.env.GITHUB_ACTIONS) process.stderr.write(`\n::${stop}::\n`);
    if (result.status === 0) recover(state, source, outputFile);
    return result.status ?? 1;
  } finally {
    fs.rmSync(work, { recursive: true, force: true });
  }
}
async function main() {
  try {
    if (process.argv[2] === "install") {
      const state = install(process.cwd());
      assert.ok(process.env.GITHUB_PATH && process.env.GITHUB_ENV, "Install needs runner environment files");
      fs.appendFileSync(process.env.GITHUB_PATH, path.join(state.stateRoot, "bin") + "\n");
      fs.appendFileSync(process.env.GITHUB_ENV, `BAMBOO_CARGO_SANDBOX=${path.join(state.stateRoot, "state.json")}\n`);
    } else {
      assert.ok(process.env.BAMBOO_CARGO_SANDBOX, "Cargo isolation is not installed");
      process.exitCode = await run(JSON.parse(fs.readFileSync(process.env.BAMBOO_CARGO_SANDBOX, "utf8")), process.argv.slice(2));
    }
  } catch (error) { console.error(error.message); process.exitCode = 1; }
}
if (require.main === module) main();
module.exports = { IMAGE, childEnv, install, snapshot, regular, copyRegular, packageFile, recover, remapMetadata, publishArchive, run };
