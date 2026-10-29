#!/usr/bin/env node
"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const os = require("node:os");
const { createHash } = require("node:crypto");
const { spawnSync } = require("node:child_process");
const { parseCounts } = require("./test-groups.cjs");
const SERIAL_STORAGE_FAMILIES = ["root_birth_tests", "host_root_create::tests", "supervisor_management_tests",
  "supervisor_management_lifetime_tests", "supervisor_proof_tests", "supervisor_management::observer_lock_order_tests"].map((name) => "v2::" + name + "::");
const SERIAL_STORAGE_FILTERS = ["root_birth_tests", "host_root_create::tests", "supervisor_management", "supervisor_proof"];
const SERIAL_STORAGE_SELECTORS = [...SERIAL_STORAGE_FAMILIES,
  "session_merge::tests::supervisor_management_race_rejects_staged_task_callbacks_and_fresh_invocation_succeeds"];
const ROOT_BIRTH_CASES = ["root_birth_existing_root_progress_and_scope_reads_remain_shared",
  "root_birth_all_entrypoints_wait_for_retained_shared_authority", "root_birth_final_shared_writer_guard_and_missing_title_fail_closed",
  "root_birth_cancel_retains_exclusive_owner_and_pending_proof_stays_closed"];
// Planner library cases have one canonical default compiler and serial owner.
const PLANNER_SERIAL = {
  "bamboo-server-tools": {
    "target": "bamboo_server_tools",
    "filter": "plan::tests::",
    "names": [
      "plan::tests::actor_runtime_is_required_for_typed_read_only_enforcement",
      "plan::tests::assignment_forbids_implementation_and_interactive_questions",
      "plan::tests::invalid_selected_context_never_saves_a_child_or_arms_a_wait",
      "plan::tests::planner_enqueue_failure_rolls_back_wait_and_deletes_unstarted_child",
      "plan::tests::planner_wait_is_armed_before_the_child_is_enqueued",
      "plan::tests::selected_context_uses_canonical_child_binding_without_changing_parent_authority",
      "plan::tests::suspension_contract_tests::disabled_suspension_blocks_normalized_subagent_launch_and_wait_before_ports",
      "plan::tests::suspension_contract_tests::disabled_suspension_blocks_plan_before_any_child_port_or_wait",
      "plan::tests::suspension_contract_tests::disabled_suspension_keeps_subagent_read_and_cancel_authority_paths",
      "plan::tests::suspension_contract_tests::ordinary_plan_keeps_original_create_wait_enqueue_order",
      "plan::tests::title_defaults_to_a_bounded_task_summary",
      "plan::tests::unsupported_selected_context_route_fails_before_persist_without_legacy_fallback"
    ]
  },
  "bamboo-engine": {
    "target": "bamboo_engine",
    "filter": "planner_artifact",
    "names": [
      "runtime::runner::loop_execution::pipeline::tests::planner_artifact_reads_preserve_child_spawn_budget_but_creates_exhaust_it",
      "session_app::child_completion_coordinator::tests::planner_artifact_completion_publishes_before_durable_resume_and_deduplicates",
      "session_app::child_completion_coordinator::tests::planner_artifact_real_boot_orphan_recovery_admits_unavailable_once_without_unsealed_report",
      "session_app::child_completion_coordinator::tests::planner_artifact_terminal_sibling_recovery_publishes_the_same_bounded_reference",
      "session_app::child_session::planner_artifact::tests::planner_artifact_binds_source_revision_and_never_grants_execution",
      "session_app::child_session::planner_artifact::tests::planner_artifact_completion_only_publishes_current_sealed_report",
      "session_app::child_session::planner_artifact::tests::planner_artifact_file_is_rebuildable_immutable_and_nofollow",
      "session_app::child_session::planner_artifact::tests::planner_artifact_large_report_metadata_is_not_limited_by_resume_budget",
      "session_app::child_session::planner_artifact::tests::planner_artifact_rejects_unsealed_nonreadonly_unknown_and_duplicate_reports",
      "session_app::child_session::planner_artifact::tests::planner_artifact_synthetic_failure_returns_no_report_or_file_but_unsealed_success_is_rejected"
    ]
  }
};

const DEFAULT_TELEMETRY_CASE = "telemetry::config::tests::explicit_enable_without_transport_feature_fails_without_starting_exporter";
// Audited from accepted dev3fe and the live-bootstrap candidate: preserve the
// existing FileInbox, current-user/Work and TicketApplication ownership. The
// stream checkpoint and Inbox/actor methods are unconditional; the new root
// bootstrap tests are cfg(test) and belong to the default storage catalog.
// Default harnesses own 20 storage, two LLM and four Unix server live cases,
// plus five fresh-input server regressions; none match the serial exclusions.
// Feature cfg expressions, 31 default owners, four contexts and 19 explicit
// additive cases remain unchanged. Default catalogs own retry and Stop tests.
// The test-utils-gated engine semaphore helper is used by the default
// reloadable-provider test in server-other via the server's dev dependency.
// Gold router and reload cases belong to existing server default shards;
// registry proof cases remain in the default bamboo-llm package owner.
// Legacy background, resume summary and per-run isolation are default engine
// tests in packages-3. Host lock coverage is server-execute; Schedule and
// Connect resolver coverage is server-other. Live resume summary controls stay
// in default engine packages-3; direct background admission stays in default
// LLM packages-2, and the reloadable provider proof stays in server-other.
// The injected project Dream endpoint A/B case stays in server-handlers.
// Root-fenced startup/response guard cases remain in the default storage owner;
// Connect and Schedule routing cases stay in server-other, reasoning resolution
// in the default engine owner, and Observer cases in server-app-state.
// No additive feature cases changed.
const FEATURE_SOURCE_SHA256 = "e64480378bb97fc6f646d26983eef67f33965b3cd2051da54bf00cf4d9897487";

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
      "fixture::startup_log_rejects_previous_launch_and_foreign_listener",
      "actual_intent_exit_before_or_after_head_never_launches_unpublished_or_unknown_work",
      "actual_admission_exit_retains_exact_prepared_run_without_worker_execution",
      "actual_result_exit_recovers_submission_before_broker_ack_without_reexecution"] },
    { name: "ticket_runtime_files", kind: "test", added: [
      "actual_stopped_native_unknown_file_is_reconciled_offline_without_rewriting_or_acceptance"] }] },
];

const SERVER_FILTERS = ["browser::", "handlers::agent::chat::", "handlers::agent::execute::",
  "handlers::settings::", "app_state::", "handlers::"];
const SERVER_LIB_SHARDS = ["server-browser", "server-chat", "server-execute", "server-settings",
  "server-app-state", "server-handlers", "server-other"];
const FEATURE_SHARDS = ["agent-core", "agent-tests-0", "agent-tests-1", "agent-tests-2", "agent-tests-3",
  ...SERVER_LIB_SHARDS, "server-tests-0", "server-tests-1",
  "packages-0", "packages-1", "packages-2", "packages-3", "packages-4"];
function packageShard(name) {
  if (name === "bamboo-agent") return "agent";
  if (name === "bamboo-server") return "server";
  return "packages-" + createHash("sha256").update(name).digest().readUInt32BE(0) % 5;
}
function defaultFeatures(owner) {
  const enabled = new Set(["default"]), pending = ["default"];
  while (pending.length) for (const feature of owner.features?.[pending.pop()] || []) {
    if (Object.hasOwn(owner.features, feature) && !enabled.has(feature)) {
      enabled.add(feature); pending.push(feature);
    }
  }
  return enabled;
}
function integrationTargets(owner, shard) {
  const agent = owner.name === "bamboo-agent", count = agent ? 4 : 2;
  const index = Number(shard.slice(shard.lastIndexOf("-") + 1));
  assert.ok(Number.isInteger(index) && index >= 0 && index < count, "Invalid integration shard");
  const enabled = defaultFeatures(owner);
  return owner.targets.filter((target) => target.test && target.kind.includes("test") &&
    (target["required-features"] || []).every((feature) => enabled.has(feature)))
    .filter((target) => {
      // Both default catalogs must be available to the additive Ticket owner.
      if (agent && ["ticket_runtime_publication", "ticket_runtime_files"].includes(target.name)) return index === 0;
      return createHash("sha256").update(target.name).digest().readUInt32BE(0) % count === index;
    }).map((target) => target.name).sort();
}
function defaultShardArguments(owner, shard) {
  const args = cargoArguments(owner);
  if (!shard) return args;
  if (shard === "agent-core") return [...args, "--lib", "--bins"];
  if (shard.startsWith("agent-tests-") || shard.startsWith("server-tests-")) {
    const targets = integrationTargets(owner, shard);
    assert.ok(targets.length, "Integration shard has no targets: " + shard);
    return [...args, ...targets.flatMap((target) => ["--test", target])];
  }
  const index = SERVER_LIB_SHARDS.indexOf(shard);
  if (index >= 0) {
    args.push("--lib");
    if (index < SERVER_FILTERS.length) args.push(SERVER_FILTERS[index]);
    args.push("--", ...SERVER_FILTERS.slice(0, index).flatMap((filter) => ["--skip", filter]));
  }
  return args;
}
function selectShard(packages, featureContexts, shard) {
  if (!shard) return { packages, featureContexts };
  assert.ok(FEATURE_SHARDS.includes(shard), "Unknown feature shard: " + shard);
  const group = shard.startsWith("agent-") ? "agent" : shard.startsWith("server-") ? "server" : shard;
  const selected = packages.filter((owner) => packageShard(owner.name) === group);
  assert.ok(selected.length, "Feature shard must contain at least one package: " + shard);
  const names = new Set(selected.map((owner) => owner.name));
  return { packages: selected, featureContexts: featureContexts.filter((context) => names.has(context.package) &&
    (context.package !== "bamboo-agent" || shard === "agent-tests-0") &&
    (context.package !== "bamboo-server" || shard === "server-other")) };
}

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
    if (!compileOnly) assert.equal(context.targets.length, 1, "Runtime filters must belong to one target");
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
  } else if (PLANNER_SERIAL[owner.name]) {
    args.push("--", ...PLANNER_SERIAL[owner.name].names.flatMap(name => ["--skip", name]));
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
function runPlannerSerial(binary, owner, catalog, options, execute = spawnSync) {
  const recipe = PLANNER_SERIAL[owner.name];
  assert.ok(recipe, "Unregistered Planner library graph");
  const selected = listCases(binary, options, execute).filter(name => name.includes(recipe.filter));
  assert.deepEqual(selected, [...recipe.names].sort(), "Planner serial catalog changed; review its owner");
  for (const [target, names] of Object.entries(catalog)) if (target !== "lib/" + recipe.target) {
    assert.ok(!names.some(name => name.includes(recipe.filter)), "Planner exclusion cannot hide another target");
  }
  const before = executableHash(binary);
  const fd = fs.openSync(options.logPath, "w");
  let result;
  try { result = execute(binary, [recipe.filter, "--test-threads=1", "--color=never"], { ...options, stdio: ["ignore", fd, fd] }); }
  finally { fs.closeSync(fd); }
  assert.ok(fs.statSync(options.logPath).size <= 16 * 1024 * 1024, "Planner serial log exceeds its bound");
  const log = fs.readFileSync(options.logPath, "utf8");
  if (options.echoLog) process.stdout.write(log);
  assert.ok(!result.error && result.status === 0, "Planner serial runtime failed");
  const counts = parseCounts(log, selected.map(name => ({ name, ignored: false })),
    catalog["lib/" + recipe.target].length - selected.length);
  assert.equal(counts.passed, selected.length);
  assert.equal(counts.failed + counts.ignored, 0);
  assert.equal(executableHash(binary), before, "Planner must retain the original compiled library");
  return { status: 0, cases: selected, executable: binary, sha256: before, counts };
}

function artifactKey(item) {
  return JSON.stringify([item.package_id, item.target.name, item.target.kind, item.features, item.profile, item.filenames, item.executable]);
}
function executableHash(file) {
  const hash = createHash("sha256"), block = Buffer.alloc(1024 * 1024), fd = fs.openSync(file, "r");
  try { let size; while ((size = fs.readSync(fd, block, 0, block.length, null))) hash.update(block.subarray(0, size)); }
  finally { fs.closeSync(fd); }
  return hash.digest("hex");
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
  const workspace = JSON.parse(metadata.stdout);
  const allPackages = selectedPackages(workspace);
  assert.equal(sourceFeatureHash(allPackages, root), sourceCatalogSha,
    "Feature-sensitive source changed; review and update its explicit case ownership before admission");
  for (const context of featureContexts) assert.ok(allPackages.some((owner) => owner.name === context.package),
    "Missing feature owner: " + context.package);
  const selection = selectShard(allPackages, featureContexts, environment.BAMBOO_FEATURE_SHARD);
  const packages = selection.packages;
  featureContexts = selection.featureContexts;
  const directory = fs.mkdtempSync(path.join(environment.RUNNER_TEMP || os.tmpdir(), "bamboo-feature-tests-"));
  const contexts = [], defaults = new Map();
  const docOwner = environment.BAMBOO_FEATURE_SHARD === "agent-core" || environment.BAMBOO_FEATURE_SHARD === "server-tests-0"
    ? packages[0] : null;
  const plan = [...packages.map((owner) => ({ label: "default-" + owner.name })),
    ...(docOwner ? [{ label: "default-doc-" + docOwner.name }, { label: "default-examples-" + docOwner.name }] : []),
    ...featureContexts.map((context) => ({ label: "feature-" + context.package, features: context.features, targets: context.targets }))];
  const finished = [], source = { root, sourceCatalogSha,
    controllerSha256: createHash("sha256").update(fs.readFileSync(__filename)).digest("hex"),
    event: Object.fromEntries(["GITHUB_REPOSITORY", "GITHUB_SHA", "GITHUB_WORKFLOW_SHA", "GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT"]
      .map((key) => [key, environment[key] || null])) };
  function checkpoint(phase, current, command = null, complete = false) {
    // Diagnostics only: never read this file as a resume/skip or acceptance input.
    // Rename within the existing evidence directory retains the last whole snapshot
    // if this controller is killed while writing the next one.
    const temporary = path.join(directory, "progress.json.tmp");
    fs.writeFileSync(temporary, JSON.stringify({ diagnosticOnly: true, complete, phase, current, command,
      source, targetDirectory: env.CARGO_TARGET_DIR, plan, finished, contexts }, null, 2) + "\n");
    fs.renameSync(temporary, path.join(directory, "progress.json"));
  }
  checkpoint("planned", null);
  function cargo(label, args) {
    checkpoint("cargo", label, ["cargo", ...args]);
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
    checkpoint("cargo-finished", label, receipt.command);
    return { receipt, log, units };
  }
  function catalog(run, owner) {
    checkpoint("catalog", run.receipt.label);
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
      run = cargo("default-" + owner.name, defaultShardArguments(owner, environment.BAMBOO_FEATURE_SHARD));
      run.receipt.catalog = catalog(run, owner);
      defaults.set(owner.name, run.receipt.catalog);
      if (owner.name === "bamboo-infrastructure") {
        assert.ok(run.receipt.catalog["lib/bamboo_infrastructure"]?.includes(DEFAULT_TELEMETRY_CASE),
          "Default-only production telemetry witness must remain compiled");
        if (run.receipt.status === 0) assert.ok(successfulCases(run.log).includes(DEFAULT_TELEMETRY_CASE),
          "Default-only production telemetry witness must execute");
      }
      if (PLANNER_SERIAL[owner.name]) {
        const recipe = PLANNER_SERIAL[owner.name];
        const unit = run.units.find(item => item.package_id === owner.id && item.target.name === recipe.target && item.profile.test);
        assert.deepEqual(unit?.features, [], "Planner's standalone default feature graph changed");
        assert.ok(unit.profile.opt_level === "0" && unit.profile.debuginfo === 0 && unit.profile.debug_assertions && unit.profile.overflow_checks,
          "Planner reuses A's canonical test profile, not an ABI/unwind variant");
        // Retain each standalone dependency graph. The frontend flag cannot
        // change either library graph: neither contains the server.
        const dependencyFeatures = owner.name === "bamboo-engine"
          ? { "bamboo-config": [], "bamboo-infrastructure": ["test-utils"] }
          : { "bamboo-config": ["test-utils"], "bamboo-infrastructure": [] };
        run.receipt.plannerDefaultGraph = Object.entries(dependencyFeatures).map(([name, features]) => {
          const dependency = workspace.packages.find(item => item.name === name);
          assert.ok(dependency, "Missing Planner graph dependency");
          const rows = run.units.filter(item => item.package_id === dependency.id && item.target.kind.includes("lib") && !item.profile.test);
          assert.equal(rows.length, 1, "Planner dependency must have one original lib unit");
          assert.deepEqual(rows[0].features, features, "Planner default graph changed; review ownership");
          return evidence(rows[0]);
        });
        const server = workspace.packages.find(item => item.name === "bamboo-server");
        assert.ok(!server || !run.units.some(item => item.package_id === server.id), "Planner library graph cannot acquire a frontend mode");
        const binary = compiledExecutable(run.log, owner, env.CARGO_TARGET_DIR, recipe.target, "lib");
        checkpoint("planner-serial", run.receipt.label);
        run.receipt.plannerSerial = runPlannerSerial(binary, owner, run.receipt.catalog,
          { ...packageOptions(owner, root, env), logPath: path.join(directory, "planner-serial-" + owner.name + ".log"), echoLog }, execute);
      }
      if (owner.name === "bamboo-storage") {
        const binary = compiledExecutable(run.log, owner, env.CARGO_TARGET_DIR, "bamboo_storage", "lib");
        checkpoint("storage-serial", run.receipt.label);
        const serial = runSerial(binary, { ...packageOptions(owner, root, env),
          logPath: path.join(directory, "storage-serial.log"), echoLog }, execute);
        run.receipt.serialStatus = serial.error ? 1 : serial.status ?? 1;
        run.receipt.status ||= run.receipt.serialStatus;
      }
      if (owner.name === "bamboo-broker") {
        const binary = sshExecutable(run.log, owner, env.CARGO_TARGET_DIR);
        const sshEnv = { ...packageOptions(owner, root, env).env, BAMBOO_RUSSH_TEST_EXECUTABLE: binary };
        checkpoint("live-ssh", run.receipt.label);
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
    finished.push("default-" + owner.name);
    checkpoint("context-finished", "default-" + owner.name);
  }
  if (docOwner) {
    const commands = [
      ["default-doc-" + docOwner.name, ["test", "--locked", "--no-fail-fast", "--message-format=json-render-diagnostics", "-p", docOwner.name, "--doc"]],
      ["default-examples-" + docOwner.name, ["build", "--locked", "--profile", "test", "--message-format=json-render-diagnostics", "-p", docOwner.name, "--examples"]],
    ];
    for (const [label, args] of commands) {
      try { cargo(label, args); }
      catch (error) { contexts.push({ label, status: 1, error: error.message }); }
      finished.push(label);
      checkpoint("context-finished", label);
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
      compile.receipt.catalog = current;
      validateFeatureCatalog(context, previous, current);
      const units = new Set(compile.units.map(artifactKey));
      const targets = context.targets.map((target) => {
        assert.ok(target.added.length, "A runtime target needs explicit additive cases");
        const executable = compiledExecutable(compile.log, owner, env.CARGO_TARGET_DIR, target.name, target.kind);
        return { target, executable, sha256: executableHash(executable) };
      });
      compile.receipt.targetExecutables = targets.map(({ target, executable, sha256 }) => ({ name: target.name, kind: target.kind, executable, sha256 }));
      for (const selected of targets) {
        const { target, executable, sha256 } = selected;
        const label = "feature-run-" + owner.name + "-" + target.kind + "-" + target.name;
        let runtime;
        try {
          // Filters belong to one harness, even when another target has the same name.
          runtime = cargo(label, cargoArguments(owner, { ...context, targets: [target] }));
          runtime.receipt.target = { name: target.name, kind: target.kind, executable, sha256 };
          assert.equal(runtime.receipt.status, 0, "Additive feature runtime failed");
          assert.deepEqual(successfulCases(runtime.log), [...target.added].sort(), "Run only this target's additive cases once");
          const keys = runtime.units.map(artifactKey);
          assert.equal(new Set(keys).size, keys.length, "Duplicate runtime compiler units");
          assert.ok(keys.every((key) => units.has(key)), "Runtime units must belong to the original compilation");
          assert.ok(runtime.units.every((item) => item.fresh), "Feature runtime must reuse its existing Cargo artifacts");
          assert.equal(compiledExecutable(runtime.log, owner, env.CARGO_TARGET_DIR, target.name, target.kind), executable);
          assert.equal(executableHash(executable), sha256, "Runtime must retain the original target executable bytes");
          runtime.receipt.cases = [...target.added].sort();
        } catch (error) {
          if (!runtime) contexts.push({ label, status: 1, error: error.message });
          else { runtime.receipt.status ||= 1; runtime.receipt.error = error.message; }
          if (echoLog) process.stderr.write(label + ": " + error.message + "\n");
        }
      }
    } catch (error) {
      if (!compile) contexts.push({ label: "feature-" + context.package, status: 1, error: error.message });
      else { compile.receipt.status ||= 1; compile.receipt.error = error.message; }
      if (echoLog) process.stderr.write(context.package + ": " + error.message + "\n");
    }
    finished.push("feature-" + context.package);
    checkpoint("context-finished", "feature-" + context.package);
  }
  for (const receipt of contexts) fs.writeFileSync(path.join(directory, receipt.label + ".json"), JSON.stringify(receipt, null, 2) + "\n");
  const status = contexts.find((context) => context.status !== 0)?.status || 0;
  if (environment.GITHUB_STEP_SUMMARY) fs.appendFileSync(environment.GITHUB_STEP_SUMMARY,
    "Feature Unit/E2E: " + packages.length + " independent default package contexts; " + featureContexts.length +
    " explicit additive feature contexts; one owned target directory. Status: " + status + ".\n");
  checkpoint("finished", null, null, status === 0 && finished.length === plan.length &&
    contexts.length === packages.length + (docOwner ? 2 : 0) + featureContexts.reduce((count, context) => count + 1 + context.targets.length, 0));
  return { status, contexts, directory, packages: packages.map((item) => item.name) };
}

module.exports = { selectedPackages, sourceFeatureHash, cargoArguments, compiledExecutable, sshExecutable, runSerialStorage,
  FEATURE_SHARDS, SERVER_FILTERS, SERVER_LIB_SHARDS, packageShard, integrationTargets, defaultShardArguments, selectShard,
  SERIAL_STORAGE_FAMILIES, SERIAL_STORAGE_FILTERS, SERIAL_STORAGE_SELECTORS, ROOT_BIRTH_CASES, PLANNER_SERIAL, runPlannerSerial,
  DEFAULT_TELEMETRY_CASE, FEATURE_SOURCE_SHA256, FEATURE_CONTEXTS, validateFeatureCatalog, runFeatureTests };
if (require.main === module) {
  try { process.exitCode = runFeatureTests().status; }
  catch (error) { process.stderr.write(error.message + "\n"); process.exitCode = 1; }
}
