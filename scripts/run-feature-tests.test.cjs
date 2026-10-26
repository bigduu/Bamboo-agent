"use strict";
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const os = require("node:os");
const { spawnSync } = require("node:child_process");
const { test } = require("node:test");
const { selectedPackages, sourceFeatureHash, cargoArguments, sshExecutable, runSerialStorage,
  SERIAL_STORAGE_FAMILIES, SERIAL_STORAGE_FILTERS, SERIAL_STORAGE_SELECTORS, ROOT_BIRTH_CASES,
  DEFAULT_TELEMETRY_CASE, FEATURE_CONTEXTS, validateFeatureCatalog, runFeatureTests } = require("./run-feature-tests.cjs");

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
    ...SERIAL_STORAGE_FAMILIES.slice(1).map((prefix) => prefix + "selected"), SERIAL_STORAGE_SELECTORS.at(-1)].sort();
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
  assert.equal(FEATURE_CONTEXTS.flatMap((context) => context.targets.flatMap((target) => target.added)).length, 18);
  for (const context of FEATURE_CONTEXTS) {
    const args = cargoArguments({ name: context.package }, context);
    assert.ok(!args.includes("--all-features"));
    assert.deepEqual(args.slice(args.indexOf("--") + 1),
      [...context.targets.flatMap((target) => target.added), "--exact", "--color=never"]);
    assert.equal(new Set(context.targets.flatMap((target) => target.added)).size,
      context.targets.flatMap((target) => target.added).length);
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
  assert.equal(calls.filter(([command, subcommand]) => command === "cargo" && subcommand === "test").length, 7);
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
  const featureRun = result.contexts.find((context) => context.label === "feature-run-bamboo-infrastructure");
  assert.ok(featureRun.artifacts.length > 0 && featureRun.artifacts.every((item) => item.fresh));
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
  for (const [cargoStatus, serialStatus, sshStatus] of [[23, 0, 0], [0, 17, 0], [0, 0, 19], [23, 17, 19]]) {
    let testCalls = 0, serialCalls = 0, sshCalls = 0;
    const result = runFeatureTests({ root, environment: { ...process.env, RUNNER_TEMP: root,
      CARGO_TARGET_DIR: path.join(root, "target"), GITHUB_STEP_SUMMARY: "" }, echoLog: false, featureContexts: [],
      sourceCatalogSha: sourceFeatureHash(packages, root),
      execute: (command, args, options) => {
        if (command !== "cargo") return { status: 0, stdout: serialNames().map((name) => name + ": test\n").join("") };
        if (args[0] === "metadata") return { status: 0, stdout: JSON.stringify(metadata) };
        assert.equal(args[0], "test"); testCalls++;
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
  }
});
