#!/usr/bin/env node
"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const os = require("node:os");
const { spawnSync } = require("node:child_process");
const SERIAL_STORAGE_FAMILIES = ["root_birth_tests", "host_root_create::tests", "supervisor_management_tests",
  "supervisor_management_lifetime_tests", "supervisor_proof_tests"].map((name) => `v2::${name}::`);
const SERIAL_STORAGE_FILTERS = ["root_birth_tests", "host_root_create::tests", "supervisor_management", "supervisor_proof"];
const SERIAL_STORAGE_SELECTORS = [...SERIAL_STORAGE_FAMILIES,
  "session_merge::tests::supervisor_management_race_rejects_staged_task_callbacks_and_fresh_invocation_succeeds"];
const ROOT_BIRTH_CASES = ["root_birth_existing_root_progress_and_scope_reads_remain_shared",
  "root_birth_all_entrypoints_wait_for_retained_shared_authority", "root_birth_final_shared_writer_guard_and_missing_title_fail_closed",
  "root_birth_cancel_retains_exclusive_owner_and_pending_proof_stays_closed"];

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
    ...packages.flatMap((item) => ["-p", item.name]), "--",
    ...SERIAL_STORAGE_SELECTORS.flatMap((name) => ["--skip", name])];
}

function canonicalFuturePath(file) {
  const tail = [];
  while (!fs.existsSync(file)) { tail.unshift(path.basename(file)); file = path.dirname(file); }
  return path.join(fs.realpathSync(file), ...tail);
}

function compiledExecutable(log, owner, targetDirectory, name, kind) {
  const artifacts = log.split("\n").flatMap((line) => {
    try { return [JSON.parse(line)]; } catch { return []; }
  }).filter((item) => item.reason === "compiler-artifact" && item.package_id === owner.id &&
    item.profile?.test === true && item.target?.name === name &&
    item.target.kind?.includes(kind) && item.executable);
  assert.equal(artifacts.length, 1, `Require the one compiled ${owner.name}/${name} executable`);
  const binary = path.resolve(artifacts[0].executable);
  const relative = path.relative(path.resolve(targetDirectory), binary);
  assert.ok(relative && !path.isAbsolute(relative) && relative.split(path.sep)[0] !== "..",
    "Executable must belong to this feature job's target directory");
  const stat = fs.lstatSync(binary);
  assert.ok(stat.isFile() && !stat.isSymbolicLink(), "Executable must be a regular Cargo artifact");
  assert.equal(fs.realpathSync(binary), binary, "Executable cannot traverse a symlink");
  return binary;
}

function sshExecutable(log, broker, targetDirectory) {
  return compiledExecutable(log, broker, targetDirectory, "russh_live", "test");
}

function runSerialStorage(binary, options, execute = spawnSync) {
  const list = execute(binary, ["--list", "--format", "terse"], { ...options, encoding: "utf8", maxBuffer: 1024 * 1024 });
  assert.equal(list.status, 0, list.stderr || "Cannot list the compiled storage Unit cases");
  const names = [...list.stdout.matchAll(/^(.+): test$/gm)].map((match) => match[1]);
  const selected = names.filter((name) => SERIAL_STORAGE_SELECTORS.some((filter) => name.includes(filter)));
  assert.deepEqual(selected, names.filter((name) => SERIAL_STORAGE_FILTERS.some((filter) => name.includes(filter))),
    "Canonical exclusions must preserve the complete original serial selector catalog");
  for (const prefix of SERIAL_STORAGE_FAMILIES) assert.ok(selected.some((name) => name.startsWith(prefix)), `Missing ${prefix}`);
  assert.deepEqual(selected.filter((name) => name.startsWith(SERIAL_STORAGE_FAMILIES[0])).map((name) => name.slice(SERIAL_STORAGE_FAMILIES[0].length)).sort(),
    [...ROOT_BIRTH_CASES].sort(), "Retain the original four Root birth authority regressions");
  const result = execute(binary, [...SERIAL_STORAGE_FILTERS, "--test-threads=1", "--color=never"],
    { ...options, encoding: "utf8", maxBuffer: 16 * 1024 * 1024 });
  const log = (result.stdout || "") + (result.stderr || "");
  fs.writeFileSync(options.logPath, log);
  if (options.echoLog) process.stdout.write(log);
  const summaries = [...log.matchAll(/test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;/g)];
  const executed = [...log.matchAll(/^test (.+) \.\.\. ok$/gm)].map((match) => match[1]);
  assert.equal(result.status, 0, "Serial Root birth acceptance failed");
  assert.equal(summaries.length, 1, "Require one serial storage result");
  assert.ok(Number(summaries[0][1]) > 4 && Number(summaries[0][2]) === 0 && Number(summaries[0][3]) === 0);
  assert.deepEqual(executed.sort(), selected.sort(), "Every serial storage case executes exactly once");
  assert.equal(executed.length, Number(summaries[0][1]));
  return result;
}

function runFeatureTests({ root = process.cwd(), environment = process.env, execute = spawnSync,
  runSsh, runSerial = runSerialStorage, echoLog = true } = {}) {
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
  const storage = packages.find((item) => item.name === "bamboo-storage");
  assert.ok(broker && broker.targets.some((target) => target.kind.includes("test") && target.name === "russh_live"),
    "Live SSH integration target is part of the same compilation");
  assert.ok(storage?.targets.some((target) => target.kind.includes("lib") && target.name === "bamboo_storage"),
    "Serial storage Unit cases belong to the same compilation");
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
  let serialStatus = 1;
  try {
    const binary = compiledExecutable(log, storage, targetDirectory, "bamboo_storage", "lib");
    const serial = runSerial(binary, { cwd: root, env, logPath: path.join(directory, "storage-serial.log"), echoLog }, execute);
    serialStatus = serial.error ? 1 : serial.status ?? 1;
  } catch (error) {
    if (echoLog) process.stderr.write(`Serial storage acceptance failed: ${error.message}\n`);
  }
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
    `Cargo status: ${cargoStatus}; serial storage status: ${serialStatus}; ignored live SSH status: ${sshStatus}.\n`);
  return { status: cargoStatus || serialStatus || sshStatus, cargoStatus, serialStatus, sshStatus,
    packages: packages.map((item) => item.name), logPath };
}

module.exports = { selectedPackages, cargoArguments, sshExecutable, compiledExecutable, runSerialStorage,
  SERIAL_STORAGE_FAMILIES, SERIAL_STORAGE_FILTERS, SERIAL_STORAGE_SELECTORS, ROOT_BIRTH_CASES, runFeatureTests };
if (require.main === module) {
  try { process.exitCode = runFeatureTests().status; }
  catch (error) { process.stderr.write(`${error.message}\n`); process.exitCode = 1; }
}
