"use strict";
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const os = require("node:os");
const { spawn, spawnSync } = require("node:child_process");
const { test } = require("node:test");
const { selectedPackages, sourceFeatureHash, cargoArguments, sshExecutable, runSerialStorage,
  SERIAL_STORAGE_FAMILIES, SERIAL_STORAGE_FILTERS, SERIAL_STORAGE_SELECTORS, ROOT_BIRTH_CASES,
  DEFAULT_TELEMETRY_CASE, FEATURE_CONTEXTS, validateFeatureCatalog, runFeatureTests, PLANNER_SERIAL, runPlannerSerial } = require("./run-feature-tests.cjs");

test("feature shards partition package targets and additive owners exactly once", () => {
  const { FEATURE_SHARDS, SERVER_FILTERS, SERVER_LIB_SHARDS, packageShard, integrationTargets, defaultShardArguments, selectShard } = require("./run-feature-tests.cjs");
  const metadata = JSON.parse(spawnSync("cargo", ["metadata", "--locked", "--format-version", "1", "--no-deps"], { encoding: "utf8" }).stdout);
  const packages = selectedPackages(metadata);
  const selections = FEATURE_SHARDS.map(shard => selectShard(packages, FEATURE_CONTEXTS, shard));
  assert.deepEqual(selections.flatMap(item => item.packages.map(owner => owner.name)).filter(name => !["bamboo-agent", "bamboo-server"].includes(name)).sort(),
    packages.map(owner => owner.name).filter(name => !["bamboo-agent", "bamboo-server"].includes(name)).sort());
  assert.deepEqual(selections.flatMap(item => item.featureContexts.map(context => context.package)).sort(), FEATURE_CONTEXTS.map(context => context.package).sort());
  for (const selection of selections) for (const context of selection.featureContexts) {
    assert.ok(selection.packages.some(owner => owner.name === context.package));
  }
  assert.equal(packageShard("bamboo-agent"), "agent");
  assert.equal(packageShard("bamboo-server"), "server");
  for (const [name, count] of [["bamboo-agent", 4], ["bamboo-server", 2]]) {
    const owner = packages.find(item => item.name === name), prefix = name === "bamboo-agent" ? "agent" : "server";
    const targets = Array.from({ length: count }, (_, index) => integrationTargets(owner, `${prefix}-tests-${index}`)).flat();
    const expected = owner.targets.filter(target => target.test && target.kind.includes("test") && !target["required-features"]?.length)
      .map(target => target.name).sort();
    assert.deepEqual(targets.sort(), expected);
    assert.equal(new Set(targets).size, targets.length);
    for (let index = 0; index < count; index++) assert.ok(integrationTargets(owner, `${prefix}-tests-${index}`).length > 0);
  }
  const agent = packages.find(item => item.name === "bamboo-agent");
  const agentFirst = integrationTargets(agent, "agent-tests-0");
  for (const name of ["ticket_runtime_files", "ticket_runtime_publication"]) assert.ok(agentFirst.includes(name));
  const server = packages.find(item => item.name === "bamboo-server");
  for (const name of ["browser::one", "handlers::agent::chat::browser::two", "handlers::agent::execute::x",
    "handlers::settings::x", "app_state::x", "handlers::other", "misc::other"]) {
    const matches = SERVER_LIB_SHARDS.filter((shard, index) => {
      const positive = index >= SERVER_FILTERS.length || name.includes(SERVER_FILTERS[index]);
      return positive && !SERVER_FILTERS.slice(0, index).some(filter => name.includes(filter));
    });
    assert.equal(matches.length, 1, `${name} must belong to exactly one server library shard`);
    assert.ok(defaultShardArguments(server, matches[0]).includes("--lib"));
  }
  assert.throws(() => selectShard(packages, FEATURE_CONTEXTS, "unknown"));
});

test("actual Cargo runs split agent and server targets, doctests and examples once", t => {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "bamboo-agent-shards-")));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  for (const dir of ["src", "tests", "examples", "server/src", "server/tests", "server/examples", "recorder/src"]) fs.mkdirSync(path.join(root, dir), { recursive: true });
  fs.writeFileSync(path.join(root, "Cargo.toml"), '[package]\nname="bamboo-agent"\nversion="0.0.0"\nedition="2021"\n[workspace]\nmembers=[".","server","recorder"]\ndefault-members=[".","server"]\n');
  fs.writeFileSync(path.join(root, "server/Cargo.toml"), '[package]\nname="bamboo-server"\nversion="0.0.0"\nedition="2021"\n[features]\ntest-utils=[]\n[[test]]\nname="env_cache_isolation"\nrequired-features=["test-utils"]\n');
  fs.writeFileSync(path.join(root, "recorder/Cargo.toml"), '[package]\nname="bamboo-tool-event-recorder-example"\nversion="0.0.0"\nedition="2021"\n');
  fs.writeFileSync(path.join(root, "recorder/src/lib.rs"), 'pub fn recorder() {}\n');
  const record = name => `use std::io::Write;writeln!(std::fs::OpenOptions::new().create(true).append(true).open(std::env::var("TEST_EVIDENCE").unwrap()).unwrap(),"${name}").unwrap();`;
  fs.writeFileSync(path.join(root, "src/lib.rs"), '/// ```\n/// assert_eq!(bamboo_agent::answer(), 42);\n/// ```\npub fn answer()->u8{42}\n#[test]fn unit(){' + record("unit") + '}\n');
  fs.writeFileSync(path.join(root, "src/main.rs"), 'fn main(){}\n#[test]fn binary(){' + record("binary") + '}\n');
  fs.writeFileSync(path.join(root, "examples/demo.rs"), 'fn main(){}\n');
  const targets = ["ticket_runtime_files", "ticket_runtime_publication", ...Array.from({ length: 20 }, (_, i) => `target_${i}`)];
  for (const name of targets) fs.writeFileSync(path.join(root, "tests", name + ".rs"), `#[test]fn check(){${record(name)}}\n`);
  const serverGroups = ["browser", "chat", "execute", "settings", "app-state", "handlers", "other"];
  fs.writeFileSync(path.join(root, "server/src/lib.rs"), '/// ```\n/// assert_eq!(bamboo_server::answer(), 42);\n/// ```\npub fn answer()->u8{42}\n' +
    'mod browser { #[test] fn check(){' + record("srv-browser") + '} }\n' +
    'mod handlers { mod agent { mod chat { #[test] fn check(){' + record("srv-chat") + '} } mod execute { #[test] fn check(){' + record("srv-execute") + '} } }' +
    ' mod settings { #[test] fn check(){' + record("srv-settings") + '} } #[test] fn other(){' + record("srv-handlers") + '} }\n' +
    'mod app_state { #[test] fn check(){' + record("srv-app-state") + '} }\n#[test] fn other(){' + record("srv-other") + '}\n');
  fs.writeFileSync(path.join(root, "server/examples/demo.rs"), 'fn main(){}\n');
  const serverTargets = Array.from({ length: 12 }, (_, i) => `srv_target_${i}`);
  for (const name of serverTargets) fs.writeFileSync(path.join(root, "server/tests", name + ".rs"), `#[test]fn check(){${record(name)}}\n`);
  fs.writeFileSync(path.join(root, "server/tests/env_cache_isolation.rs"), '#[test]fn gated(){panic!("default shards must not run gated targets")}\n');
  const baseEnv = { ...process.env, RUNNER_TEMP: root, TEST_EVIDENCE: path.join(root, "evidence"), CARGO_INCREMENTAL: "0" };
  assert.equal(spawnSync("cargo", ["generate-lockfile", "--offline"], { cwd: root, env: baseEnv }).status, 0);
  const metadata = JSON.parse(spawnSync("cargo", ["metadata", "--locked", "--format-version", "1", "--no-deps"],
    { cwd: root, env: baseEnv, encoding: "utf8" }).stdout);
  const sourceCatalogSha = sourceFeatureHash(selectedPackages(metadata), root);
  const { SERVER_LIB_SHARDS } = require("./run-feature-tests.cjs");
  for (const shard of ["agent-core", "agent-tests-0", "agent-tests-1", "agent-tests-2", "agent-tests-3", ...SERVER_LIB_SHARDS, "server-tests-0", "server-tests-1"]) {
    const environment = { ...baseEnv, BAMBOO_FEATURE_SHARD: shard, CARGO_TARGET_DIR: path.join(root, "target", shard) };
    const result = runFeatureTests({ root, environment, sourceCatalogSha, featureContexts: [], echoLog: false });
    assert.equal(result.status, 0, JSON.stringify(result.contexts));
    if (shard === "agent-core" || shard === "server-tests-0") {
      const name = shard === "agent-core" ? "bamboo-agent" : "bamboo-server";
      assert.ok(result.contexts.some(item => item.label === `default-doc-${name}` && item.status === 0));
      assert.ok(result.contexts.some(item => item.label === `default-examples-${name}` && item.status === 0));
      assert.ok(fs.existsSync(path.join(environment.CARGO_TARGET_DIR, "debug/examples/demo")));
    }
  }
  assert.deepEqual(fs.readFileSync(baseEnv.TEST_EVIDENCE, "utf8").trim().split("\n").sort(),
    ["binary", "unit", ...targets, ...serverTargets, ...serverGroups.map(name => "srv-" + name)].sort());
});

function packageMetadata(name, targets = []) { return { id: name, name, targets, features: {} }; }
function metadataFixture() {
  return { workspace_members: ["bamboo-agent", "bamboo-broker", "bamboo-storage", "bamboo-tool-event-recorder-example", "bamboo-analytics"],
    workspace_default_members: ["bamboo-agent", "bamboo-broker", "bamboo-storage"],
    packages: [packageMetadata("bamboo-agent", [{ kind: ["lib"], name: "bamboo_agent", test: true }]),
      packageMetadata("bamboo-broker", [{ kind: ["test"], name: "russh_live", test: true }]),
      packageMetadata("bamboo-storage", [{ kind: ["lib"], name: "bamboo_storage", test: true }]),
      packageMetadata("bamboo-tool-event-recorder-example"), packageMetadata("bamboo-analytics")] };
}
function serialNames() {
  return [...ROOT_BIRTH_CASES.map((name) => SERIAL_STORAGE_FAMILIES[0] + name),
    ...SERIAL_STORAGE_FAMILIES.slice(1).map((prefix) => prefix + (prefix.includes("observer_lock_order_tests") ? "observer_lock_order_rejects_non_root_slots_before_waiting_for_supervisor" : "selected")), SERIAL_STORAGE_SELECTORS.at(-1)].sort();
}

test("ordinary owners preserve isolated defaults and feature owners select only explicit additions", () => {
  const metadata = metadataFixture(), packages = selectedPackages(metadata);
  assert.deepEqual(packages.map((item) => item.name),
    ["bamboo-agent", "bamboo-broker", "bamboo-storage", "bamboo-tool-event-recorder-example"]);
  for (const owner of packages) {
    const args = cargoArguments(owner);
    assert.deepEqual(args.slice(0, 6), ["test", "--locked", "--no-fail-fast", "--message-format=json-render-diagnostics", "-p", owner.name]);
    assert.equal(args.filter((arg) => arg === "-p").length, 1);
    assert.ok(!args.some((arg) => ["--all-features", "--features", "--lib", "--tests", "--test", "--doc"].includes(arg)));
    if (owner.name === "bamboo-storage") {
      assert.deepEqual(args.slice(args.indexOf("--") + 1), SERIAL_STORAGE_SELECTORS.flatMap((name) => ["--skip", name]));
    } else assert.ok(!args.includes("--"));
  }
  assert.equal(FEATURE_CONTEXTS.flatMap((context) => context.targets.flatMap((target) => target.added)).length, 20);
  assert.ok(FEATURE_CONTEXTS.find((context) => context.package === "bamboo-infrastructure")
    .targets.find((target) => target.name === "bamboo_infrastructure").added
    .includes("telemetry::config::tests::zero_export_sampling_keeps_local_spans_without_sending_them"));
  for (const context of FEATURE_CONTEXTS) {
    const compile = cargoArguments({ name: context.package }, context, true);
    assert.ok(compile.includes("--no-run") && !compile.includes("--all-features"));
    if (context.targets.length > 1) assert.throws(() => cargoArguments({ name: context.package }, context), /one target/);
    for (const target of context.targets) {
      const args = cargoArguments({ name: context.package }, { ...context, targets: [target] });
      assert.deepEqual(args.slice(args.indexOf("--") + 1), [...target.added, "--exact", "--color=never"]);
      assert.equal(new Set(target.added).size, target.added.length);
    }
  }
  assert.throws(() => selectedPackages({ ...metadata, packages: metadata.packages.filter((item) => item.name !== "bamboo-broker") }));
  assert.throws(() => selectedPackages({ ...metadata, workspace_default_members: [...metadata.workspace_default_members, "bamboo-analytics"] }));
  metadata.packages[0].targets.push({ kind: ["example"], name: "unreviewed_example", test: true });
  assert.throws(() => selectedPackages(metadata), /Review a new test-bearing target/);
});

test("only exact regular compiler artifacts in the owned target may execute", (t) => {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "bamboo-owned-executable-")));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const target = path.join(root, "target"); fs.mkdirSync(target);
  const binary = path.join(target, "russh_live"); fs.writeFileSync(binary, "fixture");
  const owner = packageMetadata("bamboo-broker");
  const artifact = { reason: "compiler-artifact", package_id: owner.id, profile: { test: true },
    target: { name: "russh_live", kind: ["test"] }, executable: binary };
  const log = JSON.stringify(artifact);
  assert.equal(sshExecutable(log, owner, target), binary);
  for (const data of ["not an artifact", log + "\n" + log, JSON.stringify({ ...artifact, package_id: "foreign" }),
    JSON.stringify({ ...artifact, profile: { test: false } }), JSON.stringify({ ...artifact, executable: path.join(root, "outside") })]) {
    assert.throws(() => sshExecutable(data, owner, target));
  }
  fs.renameSync(binary, path.join(target, "original")); fs.symlinkSync("original", binary);
  assert.throws(() => sshExecutable(log, owner, target), /regular Cargo artifact/);
});

test("serial storage retains complete original selectors and exact successful runtime evidence", (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bamboo-storage-serial-"));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const names = serialNames(), listing = names.map((name) => name + ": test\n").join("");
  const success = names.map((name) => "test " + name + " ... ok\n").join("") +
    "test result: ok. " + names.length + " passed; 0 failed; 0 ignored;\n";
  const run = (overrides = {}) => runSerialStorage("/owned/storage-test", { logPath: path.join(root, "serial.log") },
    (_, args) => args.includes("--list") ? { status: 0, stdout: overrides.listing ?? listing }
      : { status: overrides.status ?? 0, stdout: overrides.log ?? success });
  assert.equal(run().status, 0);
  for (const overrides of [{ status: 23 }, { listing: "" }, { listing: listing.replace(names[0] + ": test\n", "") },
    { listing: listing + "other_module::supervisor_management_must_remain_serial: test\n" },
    { log: success.replace("0 ignored", "1 ignored") }, { log: success.replace("test " + names[0] + " ... ok\n", "") },
    { log: success + "test " + names[0] + " ... ok\n" }]) assert.throws(() => run(overrides));
});

test("new or removed feature cases fail closed without rerunning common defaults", () => {
  const context = { targets: [{ name: "unit", kind: "lib", added: ["feature_only"], removed: ["default_only"] }] };
  const before = { "lib/unit": ["common", "default_only"] }, after = { "lib/unit": ["common", "feature_only"] };
  validateFeatureCatalog(context, before, after);
  for (const changed of [{ "lib/unit": ["common", "feature_only", "future_unowned_case"] },
    { "lib/unit": ["feature_only"] }, { "lib/unit": ["common"] }, { "lib/other": ["feature_only"] }]) {
    assert.throws(() => validateFeatureCatalog(context, before, changed));
  }
});

test("A95 live target catalogs keep the shared helper additive only for publication", (t) => {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "bamboo-target-runtime-")));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const helper = "fixture::startup_log_rejects_previous_launch_and_foreign_listener";
  // Original run40425939556/Mbc2a artifact12702667670: publication was empty;
  // files already passed this helper in its default harness. Preserve that RED.
  const before = { "test/ticket_runtime_publication": [], "test/ticket_runtime_files": [
    "actual_native_worker_writes_only_its_git_worktree_and_submits_managed_code",
    "expired_capacity_lease_cannot_admit_same_worktree_while_native_pid_is_alive", helper,
    "inspect_uses_fresh_connection_after_idle_peer_close_without_retry"] };
  const context = FEATURE_CONTEXTS.find(item => item.package === "bamboo-agent");
  // Publication's four names came from the original failure. The files extension
  // is the existing reviewed source expectation, not an observed feature PASS.
  const after = { "test/ticket_runtime_publication": [helper,
    "actual_admission_exit_retains_exact_prepared_run_without_worker_execution",
    "actual_intent_exit_before_or_after_head_never_launches_unpublished_or_unknown_work",
    "actual_result_exit_recovers_submission_before_broker_ack_without_reexecution"].sort(),
    "test/ticket_runtime_files": [...before["test/ticket_runtime_files"],
      "actual_stopped_native_unknown_file_is_reconciled_offline_without_rewriting_or_acceptance"].sort() };
  assert.equal(context.targets[0].added.filter(name => name === helper).length, 1);
  assert.equal(context.targets[1].added.includes(helper), false);
  validateFeatureCatalog(context, before, after);
  assert.throws(() => validateFeatureCatalog({ ...context, targets: context.targets.map(target => ({
    ...target, added: target.added.filter(name => name !== helper) })) }, before, after), /Unowned additive cases/);

  const metadata = metadataFixture();
  metadata.packages[0].targets = context.targets.map(target => ({ name: target.name, kind: [target.kind], test: true }));
  metadata.packages[0].features["ticket-runtime-fixtures"] = [];
  const packages = selectedPackages(metadata), sourceCatalogSha = sourceFeatureHash(packages, root);
  for (const fault of ["none", "nonfresh", "foreign", "missing", "duplicate", "executable", "bytes", "repeated-default", "exit"]) {
    const listings = new Map(), calls = [];
    const result = runFeatureTests({ root, environment: { RUNNER_TEMP: root, CARGO_TARGET_DIR: path.join(root, "target") },
      echoLog: false, featureContexts: [context], sourceCatalogSha,
      runSsh: () => ({ status: 0 }), runSerial: () => ({ status: 0 }), execute: (command, args, options) => {
        if (command !== "cargo") return { status: 0, stdout: listings.get(command).map(name => name + ": test\n").join("") };
        if (args[0] === "metadata") return { status: 0, stdout: JSON.stringify(metadata) };
        const featured = args.includes("--features"), runtime = featured && !args.includes("--no-run");
        if (featured) calls.push(args);
        const owner = packages.find(item => item.name === args[args.indexOf("-p") + 1]);
        const selected = runtime ? owner.targets.filter(target => args[args.indexOf("--test") + 1] === target.name) : owner.targets;
        const rows = selected.map(target => {
          const binary = path.join(options.env.CARGO_TARGET_DIR, target.name + (featured ? "-feature" : ""));
          if (!runtime) fs.writeFileSync(binary, "compiled " + target.name);
          listings.set(binary, owner.name === "bamboo-agent" ? (featured ? after : before)["test/" + target.name] : ["ordinary"]);
          return { reason: "compiler-artifact", package_id: owner.id, target, features: featured ? ["ticket-runtime-fixtures"] : [],
            profile: { test: true }, fresh: runtime, filenames: [binary], executable: binary };
        });
        const publication = runtime && selected[0].name === "ticket_runtime_publication";
        if (publication && fault === "nonfresh") rows[0].fresh = false;
        if (publication && fault === "foreign") rows[0].package_id = "foreign";
        if (publication && fault === "missing") rows.length = 0;
        if (publication && fault === "duplicate") rows.push(rows[0]);
        if (publication && fault === "executable") rows[0].executable += "-foreign";
        if (publication && fault === "bytes") fs.writeFileSync(rows[0].executable, "changed");
        for (const row of rows) fs.writeSync(options.stdio[1], JSON.stringify(row) + "\n");
        if (runtime) {
          const names = args.slice(args.indexOf("--") + 1).filter(name => !name.startsWith("--"));
          if (!publication && fault === "repeated-default") names.push(helper);
          for (const name of names) fs.writeSync(options.stdio[1], "test " + name + " ... ok\n");
        }
        return { status: publication && fault === "exit" ? 23 : 0 };
      } });
    assert.equal(calls.filter(args => args.includes("--no-run")).length, 1);
    const runtimeCalls = calls.filter(args => !args.includes("--no-run"));
    assert.equal(runtimeCalls.length, 2, "the other target retains its result after a failure");
    assert.deepEqual(runtimeCalls.map(args => args[args.indexOf("--test") + 1]), context.targets.map(target => target.name));
    assert.equal(runtimeCalls[0].includes(helper), true);
    assert.equal(runtimeCalls[1].includes(helper), false, "files' default helper never repeats");
    assert.equal(result.status === 0, fault === "none", fault);
    const progress = JSON.parse(fs.readFileSync(path.join(result.directory, "progress.json"), "utf8"));
    assert.equal(progress.complete, fault === "none", fault);
    if (fault === "none") {
      const runtime = result.contexts.filter(item => item.label.startsWith("feature-run-"));
      assert.equal(runtime.length, 2);
      assert.deepEqual(runtime.map(item => item.target.name), context.targets.map(target => target.name));
      assert.ok(runtime.every(item => /^[0-9a-f]{64}$/.test(item.target.sha256)));
    }
  }
});

test("feature inventory catches new cases in inherited gated modules and required-feature targets", (t) => {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "bamboo-feature-inventory-")));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  fs.mkdirSync(path.join(root, "src/gated"), { recursive: true });
  fs.writeFileSync(path.join(root, "src/lib.rs"), '#[cfg(feature="fixture")]\n#[path="gated.rs"]\nmod gated;\nmod ordinary;\n');
  fs.writeFileSync(path.join(root, "src/gated.rs"), 'mod nested;\n');
  fs.writeFileSync(path.join(root, "src/gated/nested.rs"), '#[test]fn known_gated_case(){}\n');
  fs.writeFileSync(path.join(root, "src/ordinary.rs"), '#[test]fn known_ordinary_case(){}\n');
  const owner = { ...packageMetadata("fixture"), manifest_path: path.join(root, "Cargo.toml"), features: { fixture: [] } };
  const before = sourceFeatureHash([owner], root);
  fs.appendFileSync(path.join(root, "src/ordinary.rs"), '#[test]fn new_ordinary_case(){}\n');
  assert.equal(sourceFeatureHash([owner], root), before);
  fs.appendFileSync(path.join(root, "src/gated/nested.rs"), '#[test]fn new_inherited_feature_case(){}\n');
  const afterCase = sourceFeatureHash([owner], root);
  assert.notEqual(afterCase, before);
  fs.appendFileSync(path.join(root, "src/gated/nested.rs"), 'define_test!(new_macro_generated_case);\n');
  const afterMacro = sourceFeatureHash([owner], root);
  assert.notEqual(afterMacro, afterCase, "macro-generated additions cannot hide behind unchanged cfg declarations");
  fs.mkdirSync(path.join(root, "tests"));
  const requiredFile = path.join(root, "tests/required.rs");
  fs.writeFileSync(requiredFile, '#[test]fn required_feature_case(){}\n');
  owner.targets.push({ name: "gated_target", kind: ["test"], "required-features": ["fixture"], src_path: requiredFile });
  const afterTarget = sourceFeatureHash([owner], root);
  assert.notEqual(afterTarget, afterMacro);
  fs.appendFileSync(requiredFile, '#[test]fn new_required_feature_case(){}\n');
  assert.notEqual(sourceFeatureHash([owner], root), afterTarget);
});

test("actual Cargo preserves a default-only production witness and reuses compatible artifacts without repeating cases", (t) => {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "bamboo-feature-configurations-")));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  for (const directory of ["src", "tests", "broker/src", "broker/tests", "storage/src", "infra/src", "infra/tests",
    "recorder/src", "recorder/tests", "shared/src", "analytics/src"]) fs.mkdirSync(path.join(root, directory), { recursive: true });
  fs.writeFileSync(path.join(root, "Cargo.toml"), '[package]\nname="bamboo-agent"\nversion="0.0.0"\nedition="2021"\n[workspace]\nresolver="2"\nmembers=[".","broker","storage","infra","recorder","shared","analytics"]\ndefault-members=[".","broker","storage","infra"]\n[dependencies]\nshared-compat={path="shared"}\n');
  for (const [directory, name] of [["broker", "bamboo-broker"], ["storage", "bamboo-storage"],
    ["infra", "bamboo-infrastructure"], ["recorder", "bamboo-tool-event-recorder-example"], ["shared", "shared-compat"], ["analytics", "bamboo-analytics"]]) {
    fs.writeFileSync(path.join(root, directory, "Cargo.toml"), '[package]\nname="' + name + '"\nversion="0.0.0"\nedition="2021"\n' +
      (directory === "shared" ? "" : '[dependencies]\nshared-compat={path="../shared"}\n') +
      (directory === "infra" ? '[features]\notel-otlp=[]\n[[test]]\nname="feature_api"\npath="tests/feature_api.rs"\nrequired-features=["otel-otlp"]\n' : ""));
    fs.writeFileSync(path.join(root, directory, "src/lib.rs"), "pub fn answer()->u8{42}\n");
  }
  const record = (name) => 'use std::io::Write;writeln!(std::fs::OpenOptions::new().create(true).append(true).open(std::env::var("TEST_EVIDENCE").unwrap()).unwrap(),"' + name + '").unwrap();';
  fs.writeFileSync(path.join(root, "src/lib.rs"), '/// assert_eq!(bamboo_agent::answer(),42);\npub fn answer()->u8{42}\n#[cfg(test)]mod tests{#[test]fn library_unit(){' + record("unit-library") + '}}\n');
  fs.writeFileSync(path.join(root, "src/main.rs"), 'fn main(){}\n#[cfg(test)]mod tests{#[test]fn binary_unit(){' + record("unit-binary") + '}}\n');
  fs.writeFileSync(path.join(root, "tests/round_trip.rs"), '#[test]fn integration_round_trip(){' + record("integration") + '}\n');
  fs.writeFileSync(path.join(root, "broker/tests/russh_live.rs"), '#[test]#[ignore]fn russh_deploys_through_reverse_tunnel(){' + record("ssh-live") + '}\n');
  fs.writeFileSync(path.join(root, "recorder/tests/tool_event_lifecycle.rs"), '#[test]fn lifecycle(){' + record("recorder-lifecycle") + '}\n');
  fs.writeFileSync(path.join(root, "storage/src/lib.rs"), "mod v2{mod root_birth_tests{" +
    ROOT_BIRTH_CASES.map((name) => "#[test]fn " + name + '(){assert_eq!(std::env::var("CARGO_MANIFEST_DIR").unwrap(),env!("CARGO_MANIFEST_DIR"));assert_eq!(std::env::current_dir().unwrap(),std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")));' + record(name) + "}").join("\n") + "}\n" +
    SERIAL_STORAGE_FAMILIES.slice(1).map((prefix, index) => {
      const parts = prefix.slice(4, -2).split("::");
      return parts.map((part) => "mod " + part + "{").join("") + "#[test]fn selected(){" + record("serial-" + index) + "}" + "}".repeat(parts.length);
    }).join("\n") + '}\nmod session_merge{mod tests{#[test]fn ' + SERIAL_STORAGE_SELECTORS.at(-1).split("::").at(-1) + "(){" +
    record("serial-merge") + '}}}\n#[test]fn ordinary_storage(){' + record("storage-ordinary") + "}\n");
  const negative = DEFAULT_TELEMETRY_CASE.split("::").at(-1);
  fs.writeFileSync(path.join(root, "infra/src/lib.rs"), '#[cfg(test)]mod telemetry{mod config{mod tests{' +
    '#[cfg(not(feature="otel-otlp"))]#[test]fn ' + negative + '(){assert!(!cfg!(feature="otel-otlp"));' + record("default-production") + '}}}}\n' +
    '#[test]fn common_infrastructure(){' + record("infra-common") + '}\n' +
    '#[cfg(feature="otel-otlp")]#[test]fn feature_transport_only(){assert!(cfg!(feature="otel-otlp"));' + record("feature-transport") + '}\n');
  fs.writeFileSync(path.join(root, "infra/build.rs"), 'fn main(){let value=if std::env::var_os("CARGO_FEATURE_OTEL_OTLP").is_some(){"feature"}else{"default"};std::fs::write(std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap()).join("context.txt"),value).unwrap();println!("cargo:rerun-if-changed=build.rs");}\n');
  fs.writeFileSync(path.join(root, "infra/src/main.rs"), 'fn main(){print!("{}",include_str!(concat!(env!("OUT_DIR"),"/context.txt")));}\n');
  fs.writeFileSync(path.join(root, "infra/tests/feature_api.rs"), '#[test]fn feature_public_api(){assert_eq!(std::env::current_dir().unwrap(),std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")));let result=std::process::Command::new(env!("CARGO_BIN_EXE_bamboo-infrastructure")).output().unwrap();assert!(result.status.success());assert_eq!(result.stdout,b"feature");' + record("feature-api") + '}\n');
  fs.appendFileSync(path.join(root, "analytics/src/lib.rs"), '#[test]fn outside_acceptance(){panic!("unrelated analytics must not run")}\n');
  const env = { ...process.env, RUNNER_TEMP: root, TEST_EVIDENCE: path.join(root, "evidence"),
    CARGO_TARGET_DIR: path.join(root, "target"), CARGO_PROFILE_DEV_DEBUG: "0", CARGO_PROFILE_TEST_DEBUG: "0",
    CARGO_INCREMENTAL: "0", GITHUB_STEP_SUMMARY: path.join(root, "summary") };
  assert.equal(spawnSync("cargo", ["generate-lockfile", "--offline"], { cwd: root, env }).status, 0);
  const metadata = JSON.parse(spawnSync("cargo", ["metadata", "--locked", "--no-deps", "--format-version", "1"],
    { cwd: root, env, encoding: "utf8" }).stdout);
  const packages = selectedPackages(metadata), sourceCatalogSha = sourceFeatureHash(packages, root);
  const featureContexts = [{ package: "bamboo-infrastructure", features: ["otel-otlp"], targets: [
    { name: "bamboo_infrastructure", kind: "lib", added: ["feature_transport_only"], removed: [DEFAULT_TELEMETRY_CASE] },
    { name: "feature_api", kind: "test", added: ["feature_public_api"] }] }];
  const calls = [];
  const result = runFeatureTests({ root, environment: env, echoLog: false, featureContexts, sourceCatalogSha,
    execute: (command, args, options) => { calls.push([command, ...args]); return spawnSync(command, args, options); },
    runSsh: (binary, options) => spawnSync(binary, ["russh_deploys_through_reverse_tunnel", "--exact", "--ignored"],
      { ...options, encoding: "utf8" }) });
  assert.equal(result.status, 0, JSON.stringify(result.contexts));
  assert.equal(result.contexts.filter((context) => context.label.startsWith("default-")).length, 5);
  assert.equal(calls.filter(([command, subcommand]) => command === "cargo" && subcommand === "test").length, 8);
  const expected = ["unit-library", "unit-binary", "integration", "ssh-live", "recorder-lifecycle", "storage-ordinary",
    "serial-merge", "default-production", "infra-common", "feature-transport", "feature-api", ...ROOT_BIRTH_CASES,
    ...SERIAL_STORAGE_FAMILIES.slice(1).map((_, index) => "serial-" + index)].sort();
  assert.deepEqual(fs.readFileSync(env.TEST_EVIDENCE, "utf8").trim().split("\n").sort(), expected);
  const shared = metadata.packages.find((owner) => owner.name === "shared-compat");
  const units = result.contexts.flatMap((context) => context.artifacts || []).filter((item) => item.package_id === shared.id);
  const first = units.find((item) => !item.fresh);
  assert.ok(first);
  assert.ok(units.some((item) => item.fresh && JSON.stringify(item.filenames) === JSON.stringify(first.filenames)),
    "the identical compatible dependency artifact is actually fresh on a later package context");
  const featureRuns = result.contexts.filter((context) => context.label.startsWith("feature-run-bamboo-infrastructure-"));
  assert.equal(featureRuns.length, 2);
  assert.ok(featureRuns.every(run => run.artifacts.length > 0 && run.artifacts.every(item => item.fresh)));
  assert.ok(result.contexts.flatMap((context) => context.artifacts || []).filter((item) => item.profile.test)
    .every((item) => item.profile.debuginfo === 0 && item.profile.debug_assertions));
  assert.match(fs.readFileSync(env.GITHUB_STEP_SUMMARY, "utf8"), /5 independent default package contexts; 1 explicit additive feature contexts/);
  // Ordinary cases join existing default targets automatically. Feature-sensitive additions require ownership review.
  fs.writeFileSync(path.join(root, "tests/new_ordinary.rs"), "#[test]fn ordinary_future_case(){}\n");
  assert.equal(sourceFeatureHash(packages, root), sourceCatalogSha);
  fs.appendFileSync(path.join(root, "infra/src/lib.rs"), '#[cfg(feature="otel-otlp")]#[test]fn future_unowned_feature_case(){}\n');
  assert.notEqual(sourceFeatureHash(packages, root), sourceCatalogSha);
  let compilationCalls = 0;
  assert.throws(() => runFeatureTests({ root, environment: env, echoLog: false, featureContexts, sourceCatalogSha,
    execute: (command, args, options) => { if (args[0] === "test") compilationCalls++; return spawnSync(command, args, options); } }),
    /Feature-sensitive source changed/);
  assert.equal(compilationCalls, 0, "unowned feature additions fail before compilation or runtime");
});

test("default, serial and SSH failures propagate while other default contexts retain results", (t) => {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "bamboo-context-status-")));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const metadata = metadataFixture(), packages = selectedPackages(metadata);
  for (const [cargoStatus, serialStatus, sshStatus] of [[23, 0, 0], [0, 17, 0], [0, 0, 19], [23, 17, 19], [0, 0, 0]]) {
    let testCalls = 0, serialCalls = 0, sshCalls = 0;
    const existing = new Set(fs.readdirSync(root));
    const result = runFeatureTests({ root, environment: { ...process.env, RUNNER_TEMP: root,
      CARGO_TARGET_DIR: path.join(root, "target"), GITHUB_STEP_SUMMARY: "" }, echoLog: false, featureContexts: [],
      sourceCatalogSha: sourceFeatureHash(packages, root),
      execute: (command, args, options) => {
        if (command !== "cargo") return { status: 0, stdout: serialNames().map((name) => name + ": test\n").join("") };
        if (args[0] === "metadata") return { status: 0, stdout: JSON.stringify(metadata) };
        assert.equal(args[0], "test"); testCalls++;
        if (testCalls === 2) {
          const directory = fs.readdirSync(root).find((name) => name.startsWith("bamboo-feature-tests-") && !existing.has(name));
          const progress = JSON.parse(fs.readFileSync(path.join(root, directory, "progress.json"), "utf8"));
          assert.equal(progress.complete, false); assert.equal(progress.phase, "cargo");
          assert.deepEqual(progress.finished, ["default-bamboo-agent"]);
          assert.equal(progress.contexts[0].status, cargoStatus, "previous context survives before the next call");
          assert.equal(progress.source.root, root);
          assert.equal(progress.source.sourceCatalogSha, sourceFeatureHash(packages, root));
          assert.equal(progress.source.event.GITHUB_SHA, process.env.GITHUB_SHA || null);
          assert.equal(progress.command[progress.command.indexOf("-p") + 1], "bamboo-broker");
        }
        const name = args[args.indexOf("-p") + 1], owner = packages.find((item) => item.name === name);
        for (const target of owner.targets) {
          const binary = path.join(options.env.CARGO_TARGET_DIR, target.name); fs.writeFileSync(binary, "fixture");
          fs.writeSync(options.stdio[1], JSON.stringify({ reason: "compiler-artifact", package_id: owner.id, target,
            features: [], profile: { test: true }, fresh: false, filenames: [binary], executable: binary }) + "\n");
        }
        return { status: name === "bamboo-agent" ? cargoStatus : 0 };
      }, runSerial: () => { serialCalls++; return { status: serialStatus }; },
      runSsh: () => { sshCalls++; return { status: sshStatus }; } });
    assert.equal(result.status, cargoStatus || sshStatus || serialStatus);
    assert.equal(testCalls, packages.length); assert.equal(serialCalls, 1); assert.equal(sshCalls, 1);
    assert.equal(result.contexts.length, packages.length);
    const progress = JSON.parse(fs.readFileSync(path.join(result.directory, "progress.json"), "utf8"));
    assert.equal(progress.diagnosticOnly, true); assert.equal(progress.phase, "finished");
    assert.equal(progress.complete, result.status === 0);
    assert.deepEqual(progress.contexts, result.contexts);
  }
});

test("diagnostic snapshot completes only after the entire additive context plan succeeds", (t) => {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "bamboo-progress-additive-")));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const metadata = metadataFixture(), packages = selectedPackages(metadata), listings = new Map();
  metadata.packages[0].features.fixture = [];
  const featureContexts = [{ package: "bamboo-agent", features: ["fixture"], targets: [
    { kind: "lib", name: "bamboo_agent", added: ["extra"] }] }];
  for (const runtimeStatus of [0, 19]) {
    const existing = new Set(fs.readdirSync(root));
    const directory = () => path.join(root, fs.readdirSync(root).find(name => name.startsWith("bamboo-feature-tests-") && !existing.has(name)));
    const result = runFeatureTests({ root, environment: { RUNNER_TEMP: root, CARGO_TARGET_DIR: path.join(root, "target") },
      echoLog: false, featureContexts, sourceCatalogSha: sourceFeatureHash(packages, root),
      runSsh: () => ({ status: 0 }), runSerial: () => ({ status: 0 }), execute: (command, args, options) => {
        if (command !== "cargo") return { status: 0, stdout: listings.get(command).map(name => name + ": test\n").join("") };
        if (args[0] === "metadata") return { status: 0, stdout: JSON.stringify(metadata) };
        const featured = args.includes("--features"), runtime = featured && !args.includes("--no-run");
        if (runtime) {
          const progress = JSON.parse(fs.readFileSync(path.join(directory(), "progress.json"), "utf8"));
          assert.equal(progress.complete, false); assert.equal(progress.current, "feature-run-bamboo-agent-lib-bamboo_agent");
          assert.equal(progress.contexts.at(-1).label, "feature-build-bamboo-agent");
          assert.equal(progress.contexts.at(-1).status, 0);
        }
        const owner = packages.find(p => p.name === args[args.indexOf("-p") + 1]);
        for (const target of owner.targets) {
          const binary = path.join(options.env.CARGO_TARGET_DIR, target.name + (featured ? "-feature" : ""));
          fs.writeFileSync(binary, "fixture"); listings.set(binary, featured ? ["base", "extra"] : ["base"]);
          fs.writeSync(options.stdio[1], JSON.stringify({ reason: "compiler-artifact", package_id: owner.id, target,
            features: featured ? ["fixture"] : [], profile: { test: true }, fresh: runtime, filenames: [binary], executable: binary }) + "\n");
        }
        if (runtime) fs.writeSync(options.stdio[1], "test extra ... ok\n");
        return { status: runtime ? runtimeStatus : 0 };
      } });
    const progress = JSON.parse(fs.readFileSync(path.join(result.directory, "progress.json"), "utf8"));
    assert.equal(progress.complete, runtimeStatus === 0); assert.equal(result.status === 0, runtimeStatus === 0);
    assert.equal(progress.finished.length, packages.length + featureContexts.length);
  }
});

test("diagnostic snapshot survives termination in the next context without claiming completion", { timeout: 8000 }, async (t) => {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "bamboo-progress-cli-")));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const script = path.join(root, "fixture.cjs"), metadata = metadataFixture();
  fs.writeFileSync(script, `const fs=require('node:fs'),path=require('node:path');
const r=require(${JSON.stringify(path.join(__dirname, "run-feature-tests.cjs"))}),root=${JSON.stringify(root)},metadata=${JSON.stringify(metadata)};
let calls=0;
r.runFeatureTests({root,environment:{RUNNER_TEMP:root,CARGO_TARGET_DIR:path.join(root,'target')},echoLog:false,featureContexts:[],
sourceCatalogSha:r.sourceFeatureHash(r.selectedPackages(metadata),root),execute:(command,args,options)=>{
  if(command!=='cargo')return {status:0,stdout:'ordinary: test\\n'};
  if(args[0]==='metadata')return {status:0,stdout:JSON.stringify(metadata)};
  if(++calls===2){process.stdout.write('entered-second-context\\n');Atomics.wait(new Int32Array(new SharedArrayBuffer(4)),0,0);}
  const owner=metadata.packages.find(p=>p.name===args[args.indexOf('-p')+1]);
  for(const target of owner.targets){const binary=path.join(options.env.CARGO_TARGET_DIR,target.name);fs.writeFileSync(binary,'fixture');
    fs.writeSync(options.stdio[1],JSON.stringify({reason:'compiler-artifact',package_id:owner.id,target,features:[],profile:{test:true},fresh:false,filenames:[binary],executable:binary})+'\\n');}
  return {status:0};
}});`);
  const child = spawn(process.execPath, [script], { cwd: root, env: { PATH: process.env.PATH, HOME: root }, stdio: ["ignore", "pipe", "pipe"] });
  t.after(() => { if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL"); });
  let entered = false, observed = "";
  const signal = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => { child.kill("SIGKILL"); reject(new Error("fixture never reached its second context")); }, 5000);
    child.once("error", reject);
    child.stdout.on("data", (chunk) => { observed += chunk; if (!entered && observed.includes("entered-second-context")) { entered = true; child.kill("SIGKILL"); } });
    child.once("close", (_, signal) => { clearTimeout(timer); resolve(signal); });
  });
  assert.equal(entered, true); assert.equal(signal, "SIGKILL");
  const directory = fs.readdirSync(root).find((name) => name.startsWith("bamboo-feature-tests-"));
  const progress = JSON.parse(fs.readFileSync(path.join(root, directory, "progress.json"), "utf8"));
  assert.equal(progress.complete, false); assert.equal(progress.phase, "cargo");
  assert.equal(progress.current, "default-bamboo-broker");
  assert.deepEqual(progress.finished, ["default-bamboo-agent"]);
  assert.equal(progress.contexts.length, 1); assert.equal(progress.contexts[0].status, 0);
  assert.equal(progress.diagnosticOnly, true); assert.equal(progress.source.root, root);
  assert.equal(progress.source.sourceCatalogSha, sourceFeatureHash(selectedPackages(metadata), root));
  assert.equal(progress.source.event.GITHUB_SHA, null, "unknown local event identity stays explicit");
});

test("Planner22 default cases are excluded once and run serially from the same owned package binaries", t => {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "bamboo-planner-owner-")))
  t.after(() => fs.rmSync(root, { recursive: true, force: true }))
  const owners = Object.entries(PLANNER_SERIAL).map(([name, recipe]) => ({ ...packageMetadata(name,
    [{ name: recipe.target, kind: ["lib"], test: true }]), manifest_path: path.join(root, name, "Cargo.toml") }))
  const recorder = { ...packageMetadata("bamboo-tool-event-recorder-example"), manifest_path: path.join(root, "recorder/Cargo.toml") }
  for (const owner of [...owners, recorder]) fs.mkdirSync(path.dirname(owner.manifest_path), { recursive: true })
  const dependencies = ["bamboo-config", "bamboo-infrastructure", "bamboo-server"].map(name => packageMetadata(name))
  const metadata = { workspace_members: [...owners, recorder, ...dependencies].map(owner => owner.id),
    workspace_default_members: owners.map(owner => owner.id), packages: [...owners, recorder, ...dependencies] }
  const sourceCatalogSha = sourceFeatureHash(selectedPackages(metadata), root)
  for (const failure of [false, true]) {
    const compiled = [], runtime = [], listings = new Map()
    const result = runFeatureTests({ root, environment: { RUNNER_TEMP: root, CARGO_TARGET_DIR: path.join(root, "target") },
      sourceCatalogSha, featureContexts: [], echoLog: false, execute: (command, args, options) => {
        if (command !== "cargo") {
          if (args.includes("--list")) return { status: 0, stdout: listings.get(command).map(name => name + ": test\n").join("") }
          runtime.push({ command, args, env: options.env })
          const names = listings.get(command).filter(name => name.includes(args[0]))
          const failed = failure && command.endsWith("bamboo_engine")
          const log = "running " + names.length + " tests\n" + names.map((name, index) => "test " + name + " ... " + (failed && index === 0 ? "FAILED" : "ok") + "\n").join("") +
            "test result: " + (failed ? "FAILED" : "ok") + ". " + (names.length - Number(failed)) + " passed; " + Number(failed) + " failed; 0 ignored; 0 measured; 1 filtered out; finished in 0.01s\n"
          fs.writeSync(options.stdio[1], log); return { status: failed ? 23 : 0 }
        }
        if (args[0] === "metadata") return { status: 0, stdout: JSON.stringify(metadata) }
        const owner = metadata.packages.find(owner => owner.name === args[args.indexOf("-p") + 1]); compiled.push(owner.name)
        const recipe = PLANNER_SERIAL[owner.name]
        if (recipe) {
          assert.deepEqual(args.slice(args.indexOf("--") + 1), recipe.names.flatMap(name => ["--skip", name]))
          const binary = path.join(options.env.CARGO_TARGET_DIR, recipe.target); fs.writeFileSync(binary, "owned compiled library fixture")
          listings.set(binary, [...recipe.names, "ordinary::case"])
          const unit = { reason: "compiler-artifact", package_id: owner.id, features: [], fresh: false, target: owner.targets[0],
            profile: { test: true, opt_level: "0", debuginfo: 0, debug_assertions: true, overflow_checks: true }, filenames: [binary], executable: binary }
          fs.writeSync(options.stdio[1], JSON.stringify(unit) + "\n")
          for (const dependency of ["bamboo-config", "bamboo-infrastructure"]) {
            const features = owner.name === "bamboo-engine" ? (dependency === "bamboo-infrastructure" ? ["test-utils"] : []) :
              (dependency === "bamboo-config" ? ["test-utils"] : [])
            fs.writeSync(options.stdio[1], JSON.stringify({ ...unit, package_id: dependency, features,
              target: { name: dependency.replaceAll("-", "_"), kind: ["lib"] }, profile: { test: false }, filenames: [], executable: null }) + "\n")
          }
        }
        return { status: 0 }
      } })
    assert.deepEqual(compiled.sort(), [...owners, recorder].map(owner => owner.name).sort(), "One Cargo test invocation per original default package")
    assert.equal(runtime.length, 2, "Both library serial owners run even when one fails")
    assert.equal(new Set(runtime.map(call => call.command)).size, 2)
    assert.ok(runtime.every(call => call.args.includes("--test-threads=1")))
    assert.equal(result.status === 0, !failure)
    if (!failure) for (const owner of owners) {
      const receipt = result.contexts.find(item => item.label === "default-" + owner.name)
      assert.deepEqual(receipt.plannerSerial.cases, [...PLANNER_SERIAL[owner.name].names].sort())
      assert.equal(receipt.plannerSerial.sha256, require("node:crypto").createHash("sha256").update(fs.readFileSync(receipt.plannerSerial.executable)).digest("hex"))
    }
  }
})

test("Planner serial catalog rejects missing or duplicate cases, failed summaries and executable changes", t => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bamboo-planner-catalog-"))
  t.after(() => fs.rmSync(root, { recursive: true, force: true }))
  const owner = { name: "bamboo-engine" }, recipe = PLANNER_SERIAL[owner.name]
  const binary = path.join(root, "engine-test"), names = [...recipe.names].sort()
  const success = "running 10 tests\n" + names.map(name => "test " + name + " ... ok\n").join("") +
    "test result: ok. 10 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out; finished in 0.01s\n"
  const run = (change = {}) => {
    fs.writeFileSync(binary, "original compiled fixture")
    return runPlannerSerial(binary, owner, { ["lib/" + recipe.target]: [...names, "ordinary"], ...(change.catalog || {}) },
      { logPath: path.join(root, "serial.log") }, (_command, args, options) => {
        if (args.includes("--list")) return { status: 0, stdout: (change.names || names).map(name => name + ": test\n").join("") }
        fs.writeSync(options.stdio[1], change.log || success)
        if (change.bytes) fs.writeFileSync(binary, "replacement")
        return { status: change.status || 0 }
      })
  }
  assert.equal(run().cases.length, 10)
  for (const change of [{ names: names.slice(1) }, { names: [...names, "planner_artifact_unowned"] }, { names: [...names, names[0]] },
    { status: 23 }, { log: success.replace("0 ignored", "1 ignored") }, { log: success + "test " + names[0] + " ... ok\n" },
    { bytes: true }, { catalog: { "test/other": [names[0]] } }]) assert.throws(() => run(change))
})
