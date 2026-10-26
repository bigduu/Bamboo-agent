"use strict";
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const os = require("node:os");
const { spawnSync } = require("node:child_process");
const { test } = require("node:test");
const { selectedPackages, cargoArguments, sshExecutable, runSerialStorage,
  SERIAL_STORAGE_FAMILIES, SERIAL_STORAGE_FILTERS, SERIAL_STORAGE_SELECTORS,
  ROOT_BIRTH_CASES, runFeatureTests } = require("./run-feature-tests.cjs");

function packageMetadata(name, targets = []) { return { id: name, name, targets }; }
function metadataFixture() {
  return { workspace_members: ["bamboo-agent", "bamboo-broker", "bamboo-storage", "bamboo-tool-event-recorder-example", "bamboo-analytics"],
    workspace_default_members: ["bamboo-agent", "bamboo-broker", "bamboo-storage"],
    packages: [packageMetadata("bamboo-agent", [{ kind: ["lib"], name: "bamboo_agent", test: true }]),
      packageMetadata("bamboo-broker", [{ kind: ["test"], name: "russh_live", test: true }]),
      packageMetadata("bamboo-storage", [{ kind: ["lib"], name: "bamboo_storage", test: true }]),
      packageMetadata("bamboo-tool-event-recorder-example"), packageMetadata("bamboo-analytics")] };
}

test("feature selection retains default packages and recorder without analytics or repeated target flags", () => {
  const metadata = metadataFixture();
  const packages = selectedPackages(metadata);
  assert.deepEqual(packages.map((item) => item.name), ["bamboo-agent", "bamboo-broker", "bamboo-storage", "bamboo-tool-event-recorder-example"]);
  const args = cargoArguments(packages);
  assert.deepEqual(args.slice(0, 5), ["test", "--locked", "--all-features", "--no-fail-fast", "--message-format=json-render-diagnostics"]);
  assert.ok(!args.some((arg) => ["--lib", "--bins", "--tests", "--test", "--doc", "--all-targets"].includes(arg)));
  assert.deepEqual(args.slice(args.indexOf("--") + 1), SERIAL_STORAGE_SELECTORS.flatMap((name) => ["--skip", name]));
  assert.equal(new Set(packages.map((item) => item.id)).size, packages.length);
  assert.throws(() => selectedPackages({ ...metadata, packages: metadata.packages.filter((item) => item.name !== "bamboo-broker") }));
  assert.throws(() => selectedPackages({ ...metadata, workspace_default_members: [...metadata.workspace_default_members, "bamboo-analytics"] }));
  metadata.packages[0].targets.push({ kind: ["example"], name: "new_example_tests", test: true });
  assert.throws(() => selectedPackages(metadata), /Review a new test-bearing target/);
});

test("SSH uses only the exact regular integration executable in this job target directory", (t) => {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "bamboo-ssh-artifact-")));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const target = path.join(root, "target"); fs.mkdirSync(target);
  const binary = path.join(target, "russh_live"); fs.writeFileSync(binary, "fixture");
  const broker = packageMetadata("bamboo-broker");
  const artifact = { reason: "compiler-artifact", package_id: broker.id, profile: { test: true },
    target: { name: "russh_live", kind: ["test"] }, executable: binary };
  const log = JSON.stringify(artifact);
  assert.equal(sshExecutable(log, broker, target), binary);
  for (const data of ["not a compiler artifact", `${log}\n${log}`, JSON.stringify({ ...artifact, package_id: "foreign" }),
    JSON.stringify({ ...artifact, profile: { test: false } }),
    JSON.stringify({ ...artifact, target: { name: "russh_live", kind: ["bin"] } }),
    JSON.stringify({ ...artifact, executable: path.join(root, "outside") })]) assert.throws(() => sshExecutable(data, broker, target));
  fs.renameSync(binary, path.join(target, "original")); fs.symlinkSync("original", binary);
  assert.throws(() => sshExecutable(log, broker, target), /regular Cargo artifact/);
});

test("one actual Cargo invocation runs Unit, E2E and doctests, then reuses its ignored SSH executable", (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bamboo-feature-executor-"));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  for (const directory of ["src", "tests", "broker/src", "broker/tests", "storage/src", "recorder/src", "recorder/tests", "analytics/src"]) {
    fs.mkdirSync(path.join(root, directory), { recursive: true });
  }
  fs.writeFileSync(path.join(root, "Cargo.toml"), '[package]\nname="bamboo-agent"\nversion="0.0.0"\nedition="2021"\n[workspace]\nmembers=[".","broker","storage","recorder","analytics"]\ndefault-members=[".","broker","storage"]\n');
  for (const [directory, name] of [["broker", "bamboo-broker"], ["storage", "bamboo-storage"], ["recorder", "bamboo-tool-event-recorder-example"], ["analytics", "bamboo-analytics"]]) {
    fs.writeFileSync(path.join(root, directory, "Cargo.toml"), `[package]\nname="${name}"\nversion="0.0.0"\nedition="2021"\n`);
    fs.writeFileSync(path.join(root, directory, "src/lib.rs"), "pub fn answer()->u8{42}\n");
  }
  const record = (name) => `use std::io::Write;writeln!(std::fs::OpenOptions::new().create(true).append(true).open(std::env::var("TEST_EVIDENCE").unwrap()).unwrap(),"${name}").unwrap();`;
  const storageCases = [
    ...ROOT_BIRTH_CASES.map(name => `#[test]fn ${name}(){${record(name)}}`),
  ];
  fs.writeFileSync(path.join(root, "storage/src/lib.rs"), `mod v2{mod root_birth_tests{${storageCases.join("\n")}}\n` +
    SERIAL_STORAGE_FAMILIES.slice(1).map((prefix, index) => {
      const parts = prefix.slice(4, -2).split("::");
      return parts.map(part => `mod ${part}{`).join("") + `#[test]fn selected(){${record(`serial-${index}`)}}` + "}".repeat(parts.length);
    }).join("\n") + `}\nmod session_merge{mod tests{#[test]fn ${SERIAL_STORAGE_SELECTORS.at(-1).split("::").at(-1)}(){${record("serial-merge")}}}}\n` +
    `#[test]fn outside_serial_families(){${record("storage-ordinary")}}\n`);
  fs.writeFileSync(path.join(root, "src/lib.rs"), `/// assert_eq!(bamboo_agent::answer(), 42);\npub fn answer()->u8{42}\n#[cfg(test)]mod tests{#[test]fn unit_library(){${record("unit-library")}}}\n`);
  fs.writeFileSync(path.join(root, "src/main.rs"), `fn main(){}\n#[cfg(test)]mod tests{#[test]fn unit_binary(){${record("unit-binary")}}}\n`);
  fs.writeFileSync(path.join(root, "tests/round_trip.rs"), `#[test]fn integration_round_trip(){${record("integration")}}\n`);
  fs.writeFileSync(path.join(root, "broker/tests/russh_live.rs"), `#[test]#[ignore]fn russh_deploys_through_reverse_tunnel(){${record("ssh-live")}}\n`);
  fs.writeFileSync(path.join(root, "recorder/tests/tool_event_lifecycle.rs"), `#[test]fn recorder_lifecycle(){${record("recorder-lifecycle")}}\n`);
  fs.appendFileSync(path.join(root, "analytics/src/lib.rs"), '#[test]fn unrelated_analytics_must_not_run(){panic!("outside the selected package boundary")}\n');
  const evidence = path.join(root, "evidence");
  const env = { ...process.env, RUNNER_TEMP: root, TEST_EVIDENCE: evidence, CARGO_TARGET_DIR: path.join(root, "target"),
    CARGO_PROFILE_DEV_DEBUG: "0", CARGO_PROFILE_TEST_DEBUG: "0", CARGO_INCREMENTAL: "0",
    GITHUB_STEP_SUMMARY: path.join(root, "summary") };
  const lock = spawnSync("cargo", ["generate-lockfile", "--offline"], { cwd: root, env, encoding: "utf8" });
  assert.equal(lock.status, 0, lock.stderr);
  const calls = [];
  const result = runFeatureTests({ root, environment: env, echoLog: false,
    execute: (command, args, options) => { calls.push([command, ...args]); return spawnSync(command, args, options); },
    runSsh: (binary, options) => {
      assert.equal(options.env.BAMBOO_RUSSH_TEST_EXECUTABLE, binary);
      return spawnSync(binary, ["russh_deploys_through_reverse_tunnel", "--exact", "--ignored"], { ...options, encoding: "utf8" });
    } });
  assert.equal(result.status, 0, fs.readFileSync(result.logPath, "utf8"));
  const artifacts = fs.readFileSync(result.logPath, "utf8").split("\n").flatMap(line => {
    try { return [JSON.parse(line)]; } catch { return []; }
  }).filter(item => item.reason === "compiler-artifact" && item.profile.test);
  assert.ok(artifacts.length > 0);
  assert.ok(artifacts.every(item => item.profile.debuginfo === 0 && item.profile.debug_assertions === true),
    "the feature profile removes debug symbols while retaining runtime debug assertions");
  assert.equal(calls.filter(([command, subcommand]) => command === "cargo" && subcommand === "test").length, 1);
  assert.deepEqual(fs.readFileSync(evidence, "utf8").trim().split("\n").sort(),
    ["integration", "recorder-lifecycle", "ssh-live", "unit-binary", "unit-library", "storage-ordinary",
      "serial-merge", ...ROOT_BIRTH_CASES, ...SERIAL_STORAGE_FAMILIES.slice(1).map((_, index) => `serial-${index}`)].sort());
  const storageCalls = calls.filter(([command]) => /bamboo_storage-/u.test(command));
  assert.equal(storageCalls.length, 2, "listing and one serial execution reuse the compiled library test");
  assert.deepEqual(storageCalls[1].slice(1), [...SERIAL_STORAGE_FILTERS, "--test-threads=1", "--color=never"]);
  assert.match(fs.readFileSync(env.GITHUB_STEP_SUMMARY, "utf8"), /one all-features Cargo test invocation for 4 packages/);
});

test("Cargo, serial storage and SSH failures remain failures without another Cargo invocation", (t) => {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "bamboo-feature-status-")));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  for (const [cargoStatus, serialStatus, sshStatus] of [[0, 0, 23], [0, 17, 0], [101, 0, 0], [101, 17, 23]]) {
    let testCalls = 0, serialCalls = 0, sshCalls = 0;
    const result = runFeatureTests({ root, environment: { ...process.env, RUNNER_TEMP: root,
      CARGO_TARGET_DIR: path.join(root, "target"), GITHUB_STEP_SUMMARY: "" }, echoLog: false,
      execute: (command, args, options) => {
        assert.equal(command, "cargo");
        if (args[0] === "metadata") return { status: 0, stdout: JSON.stringify(metadataFixture()) };
        assert.equal(args[0], "test"); testCalls++;
        const binary = path.join(options.env.CARGO_TARGET_DIR, "russh_live"); fs.writeFileSync(binary, "fixture");
        fs.writeSync(options.stdio[1], JSON.stringify({ reason: "compiler-artifact", package_id: "bamboo-broker",
          target: { name: "russh_live", kind: ["test"] }, profile: { test: true }, executable: binary }) + "\n");
        const storage = path.join(options.env.CARGO_TARGET_DIR, "bamboo_storage"); fs.writeFileSync(storage, "fixture");
        fs.writeSync(options.stdio[1], JSON.stringify({ reason: "compiler-artifact", package_id: "bamboo-storage",
          target: { name: "bamboo_storage", kind: ["lib"] }, profile: { test: true }, executable: storage }) + "\n");
        return { status: cargoStatus };
      }, runSerial: () => { serialCalls++; return { status: serialStatus }; },
      runSsh: () => { sshCalls++; return { status: sshStatus }; } });
    assert.equal(result.status, cargoStatus || serialStatus || sshStatus);
    assert.equal(testCalls, 1); assert.equal(serialCalls, 1); assert.equal(sshCalls, 1);
  }
});

test("serial storage requires complete original families and exact successful runtime evidence", (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bamboo-storage-serial-"));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const names = [...ROOT_BIRTH_CASES.map(name => SERIAL_STORAGE_FAMILIES[0] + name),
    ...SERIAL_STORAGE_FAMILIES.slice(1).map(prefix => prefix + "selected"), SERIAL_STORAGE_SELECTORS.at(-1)];
  const listing = names.map(name => `${name}: test\n`).join("");
  const success = names.map(name => `test ${name} ... ok\n`).join("") + `test result: ok. ${names.length} passed; 0 failed; 0 ignored;\n`;
  const run = (overrides = {}) => runSerialStorage("/owned/storage-test", { logPath: path.join(root, "serial.log") },
    (_, args) => args.includes("--list") ? { status: 0, stdout: overrides.listing ?? listing }
      : { status: overrides.status ?? 0, stdout: overrides.log ?? success });
  assert.equal(run().status, 0);
  for (const overrides of [{ status: 23 }, { listing: "" }, { listing: listing.replace(names[0] + ": test\n", "") },
    { listing: listing + "other_module::supervisor_management_must_remain_serial: test\n" },
    { log: success.replace("0 ignored", "1 ignored") }, { log: success.replace(`test ${names[0]} ... ok\n`, "") },
    { log: success + `test ${names[0]} ... ok\n` }]) assert.throws(() => run(overrides));
});
