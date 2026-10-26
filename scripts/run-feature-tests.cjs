#!/usr/bin/env node
"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const os = require("node:os");
const { createHash } = require("node:crypto");
const { spawnSync } = require("node:child_process");
const SERIAL_STORAGE_FAMILIES = ["root_birth_tests", "host_root_create::tests", "supervisor_management_tests",
  "supervisor_management_lifetime_tests", "supervisor_proof_tests"].map((name) => "v2::" + name + "::");
const SERIAL_STORAGE_FILTERS = ["root_birth_tests", "host_root_create::tests", "supervisor_management", "supervisor_proof"];
const SERIAL_STORAGE_SELECTORS = [...SERIAL_STORAGE_FAMILIES,
  "session_merge::tests::supervisor_management_race_rejects_staged_task_callbacks_and_fresh_invocation_succeeds"];
const ROOT_BIRTH_CASES = ["root_birth_existing_root_progress_and_scope_reads_remain_shared",
  "root_birth_all_entrypoints_wait_for_retained_shared_authority", "root_birth_final_shared_writer_guard_and_missing_title_fail_closed",
  "root_birth_cancel_retains_exclusive_owner_and_pending_proof_stays_closed"];
const DEFAULT_TELEMETRY_CASE = "telemetry::config::tests::explicit_enable_without_transport_feature_fails_without_starting_exporter";
const FEATURE_SOURCE_SHA256 = "26fb3ce42d5e752252cc98cf02436cf40ba1c6df6458e776007442553a5d3af5";

const FEATURE_CONTEXTS = [
  { package: "bamboo-infrastructure", features: ["otel-otlp", "test-utils"], targets: [
    { name: "bamboo_infrastructure", kind: "lib", removed: [DEFAULT_TELEMETRY_CASE], added: [
      "telemetry::config::tests::explicit_guard_shutdown_is_bounded_and_idempotent",
      ...["local_otlp_collector_receives_allowlisted_trace_bytes", "retryable_collector_status_is_sent_once",
        "ambient_otel_headers_and_resources_cannot_leak_credentials",
        "unavailable_collector_never_backpressures_spans_and_shutdown_is_bounded", "zero_sample_ratio_exports_no_roots"]
        .map((name) => "telemetry::tests::otlp::" + name)] },
    { name: "test_utils_api", kind: "test", added: ["legacy_command_environment_helpers_remain_available_at_all_public_paths"] }] },
  { package: "bamboo-tickets", features: ["test-utils"], targets: [
    { name: "workspace_files", kind: "test", added: [
      "file_changed_but_receipt_publication_failed_never_repeats_effect",
      "explicit_observed_file_ack_requires_stop_and_releases_only_without_retry_or_acceptance",
      "changed_file_stale_plan_and_one_of_two_effects_never_release_unresolved_claims",
      "metadata_failure_replays_frozen_observation_receipt_and_schema_three_remains_quarantined",
      "offline_observation_uncertain_head_keeps_readonly_until_verified_reopen"] }] },
  { package: "bamboo-server", features: ["test-utils"], targets: [
    { name: "bamboo_server", kind: "lib", added: [
      "handlers::settings::bamboo_config::credentials::tests::generic_mutations_reject_env_refs_without_changing_any_layer"] },
    { name: "env_cache_isolation", kind: "test", added: ["scoped_env_cache_survives_parallel_global_publication_and_restores_nested_state"] }] },
  { package: "bamboo-agent", features: ["ticket-runtime-fixtures"], targets: [
    { name: "ticket_runtime_publication", kind: "test", added: [
      "actual_intent_exit_before_or_after_head_never_launches_unpublished_or_unknown_work",
      "actual_admission_exit_retains_exact_prepared_run_without_worker_execution",
      "actual_result_exit_recovers_submission_before_broker_ack_without_reexecution"] },
    { name: "ticket_runtime_files", kind: "test", added: [
      "actual_stopped_native_unknown_file_is_reconciled_offline_without_rewriting_or_acceptance"] }] },
];

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
      "Review a new test-bearing target before expanding acceptance: " + item.name + "/" + target.name);
  }
  return packages.sort((a, b) => a.name.localeCompare(b.name, "en"));
}

function sourceFeatureHash(packages, root) {
  const files = new Map();
  function walk(directory) {
    if (!fs.existsSync(directory)) return;
    for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
      const file = path.join(directory, entry.name);
      assert.ok(!entry.isSymbolicLink(), "Review symlinks in feature-sensitive source");
      if (entry.isDirectory()) walk(file);
      else if (entry.isFile() && file.endsWith(".rs")) files.set(file, fs.readFileSync(file, "utf8"));
    }
  }
  for (const owner of packages) for (const directory of ["src", "tests"]) {
    walk(path.join(path.dirname(owner.manifest_path || path.join(root, "Cargo.toml")), directory));
  }
  const attrs = (text) => [...text.matchAll(/#\s*!?\s*\[(?:cfg|cfg_attr)\([\s\S]*?\)\s*\]/g)]
    .filter((match) => /\bfeature\s*=/u.test(match[0]));
  const modules = (text) => [...text.matchAll(/(?:#\[path\s*=\s*"([^"]+)"\]\s*)?(?:pub(?:\([^)]*\))?\s+)?mod\s+([A-Za-z_]\w*)\s*;/g)];
  const owned = new Set();
  const queue = [];
  for (const owner of packages) for (const target of owner.targets) {
    if (target["required-features"]?.length && files.has(target.src_path)) {
      owned.add(target.src_path); queue.push(target.src_path);
    }
  }
  function addModules(file, text) {
    for (const match of modules(text)) {
      const directory = path.dirname(file);
      const base = /^(?:lib|main|mod)\.rs$/u.test(path.basename(file)) ? directory : file.slice(0, -3);
      const candidates = match[1] ? [path.join(directory, match[1])]
        : [path.join(base, match[2] + ".rs"), path.join(base, match[2], "mod.rs")];
      const child = candidates.find((candidate) => files.has(candidate));
      if (child && !owned.has(child)) { owned.add(child); queue.push(child); }
    }
  }
  for (const [file, text] of files) for (const attr of attrs(text)) {
    const tail = text.slice(attr.index + attr[0].length);
    const external = tail.match(/^\s*(?:#\[[^\]]+\]\s*)*(?:pub(?:\([^)]*\))?\s+)?mod\s+[A-Za-z_]\w*\s*;/u);
    if (external) addModules(file, external[0]);
  }
  while (queue.length) { const file = queue.shift(); addModules(file, files.get(file)); }
  const records = [...files].flatMap(([file, text]) => {
    const expressions = attrs(text).map((match) => match[0].replace(/\s+/gu, " "));
    const featureLines = text.split("\n").filter((line) => /\bfeature\s*=/u.test(line)).map((line) => line.trim());
    if (!expressions.length && !featureLines.length && !owned.has(file)) return [];
    return [{ path: path.relative(root, file).split(path.sep).join("/"),
      sourceSha256: createHash("sha256").update(text).digest("hex"),
      expressions: expressions.sort(), featureLines: featureLines.sort(),
      functions: [...text.matchAll(/\bfn\s+([A-Za-z_]\w*)/g)].map((match) => match[1]).sort(),
      modules: modules(text).map((match) => [match[1] || "", match[2]]).sort() }];
  }).sort((a, b) => a.path.localeCompare(b.path, "en"));
  const features = packages.flatMap((owner) => {
    const entries = Object.entries(owner.features || {}).sort(([a], [b]) => a.localeCompare(b, "en"));
    const required = owner.targets.filter((target) => target["required-features"]?.length)
      .map((target) => [target.name, target.kind, [...target["required-features"]].sort()]);
    return entries.length || required.length ? [[owner.name, entries.map(([name, values]) => [name, [...values].sort()]), required]] : [];
  });
  return createHash("sha256").update(JSON.stringify({ features, records })).digest("hex");
}

function cargoArguments(owner, context, compileOnly = false) {
  const args = ["test", "--locked", "--no-fail-fast", "--message-format=json-render-diagnostics", "-p", owner.name];
  if (context) {
    args.push("--features", context.features.join(","));
    for (const target of context.targets) {
      assert.ok(["lib", "test"].includes(target.kind), "Review a new additive target kind");
      if (target.kind === "lib") args.push("--lib");
      else args.push("--test", target.name);
    }
    if (compileOnly) args.push("--no-run");
    else args.push("--", ...context.targets.flatMap((target) => target.added), "--exact", "--color=never");
  } else if (owner.name === "bamboo-storage") {
    args.push("--", ...SERIAL_STORAGE_SELECTORS.flatMap((name) => ["--skip", name]));
  }
  return args;
}

function canonicalFuturePath(file) {
  const tail = [];
  while (!fs.existsSync(file)) { tail.unshift(path.basename(file)); file = path.dirname(file); }
  return path.join(fs.realpathSync(file), ...tail);
}
function artifacts(log) {
  return log.split("\n").flatMap((line) => {
    try { const value = JSON.parse(line); return value.reason === "compiler-artifact" ? [value] : []; }
    catch { return []; }
  });
}
function compiledExecutable(log, owner, targetDirectory, name, kind) {
  const matches = artifacts(log).filter((item) => item.package_id === owner.id && item.profile?.test === true &&
    item.target?.name === name && item.target.kind?.includes(kind) && item.executable);
  assert.equal(matches.length, 1, "Require one compiled " + owner.name + "/" + name + " executable");
  const binary = path.resolve(matches[0].executable);
  const relative = path.relative(targetDirectory, binary);
  assert.ok(relative && !path.isAbsolute(relative) && relative.split(path.sep)[0] !== "..", "Executable must belong to this job's target");
  const stat = fs.lstatSync(binary);
  assert.ok(stat.isFile() && !stat.isSymbolicLink(), "Executable must be a regular Cargo artifact");
  assert.equal(fs.realpathSync(binary), binary, "Executable cannot traverse a symlink");
  return binary;
}
function sshExecutable(log, owner, targetDirectory) {
  return compiledExecutable(log, owner, targetDirectory, "russh_live", "test");
}
function packageOptions(owner, root, env) {
  const directory = owner.manifest_path ? path.dirname(owner.manifest_path) : root;
  return { cwd: directory, env: { ...env, CARGO_MANIFEST_DIR: directory } };
}
function listCases(binary, options, execute) {
  const result = execute(binary, ["--list", "--format", "terse"], { ...options, encoding: "utf8", maxBuffer: 1024 * 1024 });
  assert.equal(result.status, 0, result.stderr || "Cannot list compiled cases");
  const names = [...result.stdout.matchAll(/^(.+): test\r?$/gm)].map((match) => match[1].replace(/\r$/u, "")).sort();
  assert.equal(names.length, new Set(names).size, "Duplicate test names");
  return names;
}
function successfulCases(log) {
  return [...log.matchAll(/^test (.+) \.\.\. ok\r?$/gm)].map((match) => match[1]).sort();
}
function runSerialStorage(binary, options, execute = spawnSync) {
  const names = listCases(binary, options, execute);
  const selected = names.filter((name) => SERIAL_STORAGE_SELECTORS.some((filter) => name.includes(filter)));
  assert.deepEqual(selected, names.filter((name) => SERIAL_STORAGE_FILTERS.some((filter) => name.includes(filter))),
    "Canonical exclusions must preserve the original serial catalog");
  for (const prefix of SERIAL_STORAGE_FAMILIES) assert.ok(selected.some((name) => name.startsWith(prefix)), "Missing " + prefix);
  assert.deepEqual(selected.filter((name) => name.startsWith(SERIAL_STORAGE_FAMILIES[0])).map((name) => name.slice(SERIAL_STORAGE_FAMILIES[0].length)).sort(),
    [...ROOT_BIRTH_CASES].sort(), "Retain the four Root birth regressions");
  const result = execute(binary, [...SERIAL_STORAGE_FILTERS, "--test-threads=1", "--color=never"],
    { ...options, encoding: "utf8", maxBuffer: 16 * 1024 * 1024 });
  const log = (result.stdout || "") + (result.stderr || "");
  fs.writeFileSync(options.logPath, log);
  if (options.echoLog) process.stdout.write(log);
  assert.equal(result.status, 0, "Serial storage acceptance failed");
  const summaries = [...log.matchAll(/test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;/g)];
  assert.equal(summaries.length, 1);
  assert.ok(Number(summaries[0][1]) > 4 && Number(summaries[0][2]) === 0 && Number(summaries[0][3]) === 0);
  assert.deepEqual(successfulCases(log), selected, "Every serial storage case executes once");
  assert.equal(selected.length, Number(summaries[0][1]));
  return result;
}
function artifactKey(item) {
  return JSON.stringify([item.package_id, item.target.name, item.target.kind, item.features, item.profile, item.filenames]);
}
function evidence(item) {
  assert.ok(Array.isArray(item.features) && typeof item.fresh === "boolean" && item.profile && item.target);
  return { package_id: item.package_id, target: { name: item.target.name, kind: item.target.kind },
    features: item.features, profile: item.profile, fresh: item.fresh, filenames: item.filenames, executable: item.executable };
}
function validateFeatureCatalog(context, previous, current) {
  assert.ok(previous, "Require the independently tested default catalog");
  assert.deepEqual(Object.keys(current).sort(), context.targets.map((target) => target.kind + "/" + target.name).sort());
  for (const target of context.targets) {
    const key = target.kind + "/" + target.name, before = previous[key] || [], after = current[key];
    assert.deepEqual(after.filter((name) => !before.includes(name)), [...target.added].sort(), "Unowned additive cases: " + key);
    assert.deepEqual(before.filter((name) => !after.includes(name)), [...(target.removed || [])].sort(), "Unowned removed default cases: " + key);
  }
}

function runFeatureTests({ root = process.cwd(), environment = process.env, execute = spawnSync,
  runSsh, runSerial = runSerialStorage, echoLog = true, featureContexts = FEATURE_CONTEXTS,
  sourceCatalogSha = FEATURE_SOURCE_SHA256 } = {}) {
  root = fs.realpathSync(root);
  const targetDirectory = canonicalFuturePath(path.resolve(root, environment.CARGO_TARGET_DIR || "target/feature-tests"));
  const relative = path.relative(root, targetDirectory);
  assert.ok(relative && !path.isAbsolute(relative) && relative.split(path.sep)[0] !== "..", "Require one owned target inside this checkout");
  fs.mkdirSync(targetDirectory, { recursive: true });
  const env = { ...environment, CARGO_TARGET_DIR: fs.realpathSync(targetDirectory) };
  const metadata = execute("cargo", ["metadata", "--locked", "--format-version", "1", "--no-deps"],
    { cwd: root, env, encoding: "utf8", maxBuffer: 16 * 1024 * 1024 });
  assert.equal(metadata.status, 0, metadata.stderr || "Cannot inspect packages");
  const packages = selectedPackages(JSON.parse(metadata.stdout));
  assert.equal(sourceFeatureHash(packages, root), sourceCatalogSha,
    "Feature-sensitive source changed; review and update its explicit case ownership before admission");
  const directory = fs.mkdtempSync(path.join(environment.RUNNER_TEMP || os.tmpdir(), "bamboo-feature-tests-"));
  const contexts = [], defaults = new Map();
  function cargo(label, args) {
    const logPath = path.join(directory, label + ".log"), fd = fs.openSync(logPath, "wx");
    let result;
    try { result = execute("cargo", args, { cwd: root, env, stdio: ["ignore", fd, "inherit"] }); }
    finally { fs.closeSync(fd); }
    assert.ok(fs.statSync(logPath).size <= 64 * 1024 * 1024, "Cargo evidence exceeds its bound");
    const log = fs.readFileSync(logPath, "utf8"), units = artifacts(log);
    if (echoLog) process.stdout.write(log.split("\n").filter((line) => !line.startsWith("{")).join("\n"));
    const receipt = { label, command: ["cargo", ...args], targetDirectory: env.CARGO_TARGET_DIR,
      status: result.error ? 1 : result.status ?? 1, artifacts: units.map(evidence) };
    contexts.push(receipt);
    return { receipt, log, units };
  }
  function catalog(run, owner) {
    const rows = run.units.filter((item) => item.package_id === owner.id && item.profile.test && item.executable);
    const result = {};
    for (const row of rows) {
      const kind = row.target.kind.find((value) => ["lib", "bin", "test"].includes(value));
      assert.ok(kind, "Review a new harness kind");
      const key = kind + "/" + row.target.name;
      assert.ok(!result[key], "Duplicate harness");
      const binary = compiledExecutable(run.log, owner, env.CARGO_TARGET_DIR, row.target.name, kind);
      result[key] = listCases(binary, packageOptions(owner, root, env), execute);
    }
    return result;
  }
  for (const owner of packages) {
    let run;
    try {
      run = cargo("default-" + owner.name, cargoArguments(owner));
      run.receipt.catalog = catalog(run, owner);
      defaults.set(owner.name, run.receipt.catalog);
      if (owner.name === "bamboo-infrastructure") {
        assert.ok(run.receipt.catalog["lib/bamboo_infrastructure"]?.includes(DEFAULT_TELEMETRY_CASE),
          "Default-only production telemetry witness must remain compiled");
        if (run.receipt.status === 0) assert.ok(successfulCases(run.log).includes(DEFAULT_TELEMETRY_CASE),
          "Default-only production telemetry witness must execute");
      }
      if (owner.name === "bamboo-storage") {
        const binary = compiledExecutable(run.log, owner, env.CARGO_TARGET_DIR, "bamboo_storage", "lib");
        const serial = runSerial(binary, { ...packageOptions(owner, root, env),
          logPath: path.join(directory, "storage-serial.log"), echoLog }, execute);
        run.receipt.serialStatus = serial.error ? 1 : serial.status ?? 1;
        run.receipt.status ||= run.receipt.serialStatus;
      }
      if (owner.name === "bamboo-broker") {
        const binary = sshExecutable(run.log, owner, env.CARGO_TARGET_DIR);
        const sshEnv = { ...packageOptions(owner, root, env).env, BAMBOO_RUSSH_TEST_EXECUTABLE: binary };
        const ssh = runSsh ? runSsh(binary, { cwd: root, env: sshEnv })
          : execute("bash", ["scripts/run-russh-live.sh"], { cwd: root, env: sshEnv, stdio: "inherit" });
        run.receipt.sshStatus = ssh.error ? 1 : ssh.status ?? 1;
        run.receipt.status ||= run.receipt.sshStatus;
      }
    } catch (error) {
      if (!run) contexts.push({ label: "default-" + owner.name, status: 1, error: error.message });
      else { run.receipt.status ||= 1; run.receipt.error = error.message; }
      if (echoLog) process.stderr.write(owner.name + ": " + error.message + "\n");
    }
  }
  for (const context of featureContexts) {
    let compile;
    try {
      const owner = packages.find((item) => item.name === context.package);
      assert.ok(owner, "Missing feature owner: " + context.package);
      compile = cargo("feature-build-" + owner.name, cargoArguments(owner, context, true));
      assert.equal(compile.receipt.status, 0, "Feature compilation failed");
      const current = catalog(compile, owner), previous = defaults.get(owner.name);
      validateFeatureCatalog(context, previous, current);
      const runtime = cargo("feature-run-" + owner.name, cargoArguments(owner, context));
      assert.equal(runtime.receipt.status, 0, "Additive feature runtime failed");
      assert.deepEqual(successfulCases(runtime.log), context.targets.flatMap((target) => target.added).sort(),
        "Run every additive case once and no ordinary case twice");
      assert.deepEqual(runtime.units.map(artifactKey).sort(), compile.units.map(artifactKey).sort(), "Feature runtime must reuse the compiled unit set");
      assert.ok(runtime.units.every((item) => item.fresh), "Feature runtime must reuse its existing Cargo artifacts");
      runtime.receipt.cases = context.targets.flatMap((target) => target.added).sort();
    } catch (error) {
      if (!compile) contexts.push({ label: "feature-" + context.package, status: 1, error: error.message });
      else { compile.receipt.status ||= 1; compile.receipt.error = error.message; }
      if (echoLog) process.stderr.write(context.package + ": " + error.message + "\n");
    }
  }
  for (const receipt of contexts) fs.writeFileSync(path.join(directory, receipt.label + ".json"), JSON.stringify(receipt, null, 2) + "\n");
  const status = contexts.find((context) => context.status !== 0)?.status || 0;
  if (environment.GITHUB_STEP_SUMMARY) fs.appendFileSync(environment.GITHUB_STEP_SUMMARY,
    "Feature Unit/E2E: " + packages.length + " independent default package contexts; " + featureContexts.length +
    " explicit additive feature contexts; one owned target directory. Status: " + status + ".\n");
  return { status, contexts, directory, packages: packages.map((item) => item.name) };
}

module.exports = { selectedPackages, sourceFeatureHash, cargoArguments, compiledExecutable, sshExecutable, runSerialStorage,
  SERIAL_STORAGE_FAMILIES, SERIAL_STORAGE_FILTERS, SERIAL_STORAGE_SELECTORS, ROOT_BIRTH_CASES,
  DEFAULT_TELEMETRY_CASE, FEATURE_SOURCE_SHA256, FEATURE_CONTEXTS, validateFeatureCatalog, runFeatureTests };
if (require.main === module) {
  try { process.exitCode = runFeatureTests().status; }
  catch (error) { process.stderr.write(error.message + "\n"); process.exitCode = 1; }
}
