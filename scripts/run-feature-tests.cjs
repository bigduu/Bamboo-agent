#!/usr/bin/env node
"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const os = require("node:os");
const { spawnSync } = require("node:child_process");

function selectedPackages(metadata) {
  const selected = new Set(metadata.workspace_default_members);
  const recorder = metadata.packages.find((item) => item.name === "bamboo-tool-event-recorder-example");
  assert.ok(recorder && metadata.workspace_members.includes(recorder.id), "Require the existing recorder package");
  selected.add(recorder.id);
  const packages = metadata.packages.filter((item) => selected.has(item.id));
  assert.equal(packages.length, selected.size, "Missing selected workspace package");
  assert.ok(packages.length > 0 && packages.every((item) => item.name !== "bamboo-analytics"),
    "Feature acceptance must retain the existing package boundary");
  for (const item of packages) for (const target of item.targets.filter((target) => target.test)) {
    assert.ok(target.kind.some((kind) => ["lib", "bin", "test"].includes(kind)),
      `Review a new test-bearing target before expanding acceptance: ${item.name}/${target.name}`);
  }
  return packages.sort((a, b) => a.name.localeCompare(b.name, "en"));
}

function cargoArguments(packages) {
  // Cargo's default test selection runs lib/bin/integration targets and doctests
  // once. Do not combine --doc with explicit target flags or invoke Cargo again.
  return ["test", "--locked", "--all-features", "--no-fail-fast", "--message-format=json-render-diagnostics",
    ...packages.flatMap((item) => ["-p", item.name])];
}

function canonicalFuturePath(file) {
  const tail = [];
  while (!fs.existsSync(file)) { tail.unshift(path.basename(file)); file = path.dirname(file); }
  return path.join(fs.realpathSync(file), ...tail);
}

function sshExecutable(log, broker, targetDirectory) {
  const artifacts = log.split("\n").flatMap((line) => {
    try { return [JSON.parse(line)]; } catch { return []; }
  }).filter((item) => item.reason === "compiler-artifact" && item.package_id === broker.id &&
    item.profile?.test === true && item.target?.name === "russh_live" &&
    item.target.kind?.includes("test") && item.executable);
  assert.equal(artifacts.length, 1, "Require the one compiled live SSH integration executable");
  const binary = path.resolve(artifacts[0].executable);
  const relative = path.relative(path.resolve(targetDirectory), binary);
  assert.ok(relative && !path.isAbsolute(relative) && relative.split(path.sep)[0] !== "..",
    "SSH executable must belong to this feature job's target directory");
  const stat = fs.lstatSync(binary);
  assert.ok(stat.isFile() && !stat.isSymbolicLink(), "SSH executable must be a regular Cargo artifact");
  assert.equal(fs.realpathSync(binary), binary, "SSH executable cannot traverse a symlink");
  return binary;
}

function runFeatureTests({ root = process.cwd(), environment = process.env, execute = spawnSync,
  runSsh, echoLog = true } = {}) {
  root = fs.realpathSync(root);
  let targetDirectory = canonicalFuturePath(path.resolve(root, environment.CARGO_TARGET_DIR || "target/feature-all-features"));
  const relative = path.relative(root, targetDirectory);
  assert.ok(relative && !path.isAbsolute(relative) && relative.split(path.sep)[0] !== "..",
    "Feature tests require one owned target directory inside their checkout");
  fs.mkdirSync(targetDirectory, { recursive: true });
  targetDirectory = fs.realpathSync(targetDirectory);
  const env = { ...environment, CARGO_TARGET_DIR: targetDirectory };
  const metadataResult = execute("cargo", ["metadata", "--locked", "--all-features", "--format-version", "1", "--no-deps"],
    { cwd: root, env, encoding: "utf8", maxBuffer: 16 * 1024 * 1024 });
  assert.equal(metadataResult.status, 0, metadataResult.stderr || "Cannot inspect selected Cargo targets");
  const packages = selectedPackages(JSON.parse(metadataResult.stdout));
  const broker = packages.find((item) => item.name === "bamboo-broker");
  assert.ok(broker && broker.targets.some((target) => target.kind.includes("test") && target.name === "russh_live"),
    "Live SSH integration target is part of the same compilation");
  const temporary = environment.RUNNER_TEMP || os.tmpdir();
  const directory = fs.mkdtempSync(path.join(temporary, "bamboo-feature-tests-"));
  const logPath = path.join(directory, "cargo.log");
  const fd = fs.openSync(logPath, "wx");
  let result;
  try { result = execute("cargo", cargoArguments(packages), { cwd: root, env, stdio: ["ignore", fd, "inherit"] }); }
  finally { fs.closeSync(fd); }
  assert.ok(fs.statSync(logPath).size <= 64 * 1024 * 1024, "Feature Cargo evidence exceeds its bound");
  const log = fs.readFileSync(logPath, "utf8");
  if (echoLog) process.stdout.write(log);
  const cargoStatus = result.error ? 1 : result.status ?? 1;
  let sshStatus = 1;
  try {
    const binary = sshExecutable(log, broker, targetDirectory);
    const sshEnv = { ...env, BAMBOO_RUSSH_TEST_EXECUTABLE: binary };
    const ssh = runSsh ? runSsh(binary, { cwd: root, env: sshEnv })
      : execute("bash", ["scripts/run-russh-live.sh"], { cwd: root, env: sshEnv, stdio: "inherit" });
    sshStatus = ssh.error ? 1 : ssh.status ?? 1;
  } catch (error) {
    if (echoLog) process.stderr.write(`SSH acceptance failed: ${error.message}\n`);
  }
  if (environment.GITHUB_STEP_SUMMARY) fs.appendFileSync(environment.GITHUB_STEP_SUMMARY,
    `Feature Unit/E2E: one all-features Cargo test invocation for ${packages.length} packages.\n` +
    `Cargo status: ${cargoStatus}; ignored live SSH status: ${sshStatus}.\n`);
  return { status: cargoStatus || sshStatus, cargoStatus, sshStatus, packages: packages.map((item) => item.name), logPath };
}

module.exports = { selectedPackages, cargoArguments, sshExecutable, runFeatureTests };
if (require.main === module) {
  try { process.exitCode = runFeatureTests().status; }
  catch (error) { process.stderr.write(`${error.message}\n`); process.exitCode = 1; }
}
