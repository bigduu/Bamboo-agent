"use strict";
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { createHash } = require("node:crypto");
const test = require("node:test");
const { ACTOR_TOOLCHAIN, TLS, FRONTEND, TICKET, recipes, childEnvironment, fileIdentity,
  selectedCases, assertRuntime, cargoArguments, observeArtifacts, assertActorCounts, runNativeOwner } = require("./native-test-owner.cjs");

const actorSource = fs.readFileSync(path.join(__dirname, "actor-windows-native.ps1"), "utf8");
const actorScopes = [...actorSource.matchAll(/Assert-TestList '([^']+)' '([^']+)' @\(([\s\S]*?)\) '[^']+'/gu)].map(match => ({
  package: match[1], filter: match[2], names: [...match[3].matchAll(/'([^']+)'/gu)].map(name => match[2] + name[1]),
}));
const digest = file => createHash("sha256").update(fs.readFileSync(file)).digest("hex");

function cases(recipe) {
  if (recipe.id === "server-default") return [
    ...Array.from({ length: 7 }, (_, index) => TLS + "fixture_" + index),
    ...Array.from({ length: 7 }, (_, index) => FRONTEND + "fixture_" + index),
  ];
  const required = recipe.unwind ? [TLS + "macos_test_link_keeps_dwarf_backtraces"]
    : recipe.recorder ? ["native_recorder_lifecycle_is_bounded_and_generation_safe", "boot_reconcile_restarts_the_installed_native_recorder"]
      : recipe.id === "ticket-default" ? [TICKET]
        : recipe.id === "skills-default" ? (recipe.count === 39
          ? [SKILL("windows_real_opened_handle_identity_admits_and_reuses_regular_sources"), SKILL("windows_real_directory_junction_cannot_supply_instruction_policy"), "progressive::read_tests::actual_windows_junction_is_created_and_rejected_without_reading_target"]
          : [SKILL("file_and_directory_links_and_fifo_cannot_supply_policy_or_main"), "progressive::read_tests::unix_file_directory_link_fifo_and_read_error_do_not_become_empty_or_absent"])
          : recipe.id === "skill-tool-default" && recipe.count === 46 ? ["skill_runtime::catalog_tests::skills_read_native_windows_junction_replacement_rejects_foreign_auxiliary_data"] : [];
  return [...required, ...Array.from({ length: recipe.count - required.length }, (_, index) => (recipe.filters[0] || FRONTEND) + "fixture_" + index)];
}
function SKILL(name) { return "progressive::source_tests::" + name; }

test("fixed native owners retain separate default, Actor API-only and macOS dev profiles", () => {
  assert.deepEqual(actorScopes.map(scope => scope.names.length), [9, 3, 19, 5, 5]);
  assert.equal(actorScopes.flatMap(scope => scope.names).length, 41);
  assert.equal(recipes("win32")[0].toolchain, ACTOR_TOOLCHAIN);
  assert.equal(recipes("win32")[0].frontend, "api-only");
  assert.equal(recipes("darwin")[0].profile, "dev");
  for (const platform of ["win32", "darwin"]) for (const recipe of recipes(platform).filter(recipe => !recipe.actor)) {
    const args = cargoArguments(recipe);
    assert.equal(args.filter(arg => arg === "-p").length, 1);
    assert.ok(!args.includes("--workspace"));
    assert.equal(args.includes("--all-features"), Boolean(recipe.unwind));
    assert.equal(recipe.frontend, "embedded");
  }
  assert.throws(() => recipes("linux"));
});

test("native subprocesses receive their selected context without credentials or workflow write channels", () => {
  const input = { PATH: "fixture", RUSTUP_HOME: "rustup", CARGO_HOME: "cargo", GITHUB_SHA: "a".repeat(40),
    GH_TOKEN: "private", CARGO_REGISTRY_TOKEN: "private", OPENAI_API_KEY: "private", BAMBOO_RELEASE_SIGNING_KEY: "private",
    ANTHROPIC_KEY: "private", SSH_AUTH_SOCK: "private", PROVIDER_COOKIE: "private",
    TUNNEL_URL: "private", HTTPS_PROXY: "private", GITHUB_OUTPUT: "private", GITHUB_ENV: "private", GITHUB_STEP_SUMMARY: "private",
    BAMBOO_FRONTEND_BUILD_MODE: "api-only" };
  const result = childEnvironment(input, "owned", recipes("win32")[1]);
  assert.equal(result.CARGO_TARGET_DIR, "owned"); assert.equal(result.RUSTUP_TOOLCHAIN, "stable");
  assert.ok(!Object.keys(result).some(key => /TOKEN|KEY|AUTH|COOKIE|TUNNEL|PROXY/u.test(key)));
  assert.ok(!("GITHUB_OUTPUT" in result) && !("GITHUB_ENV" in result) && !("BAMBOO_FRONTEND_BUILD_MODE" in result));
  assert.equal(childEnvironment(input, "owned", recipes("win32")[0]).BAMBOO_FRONTEND_BUILD_MODE, "api-only");
  assert.equal(input.GH_TOKEN, "private", "Do not mutate the host environment");
});

test("actual named selection and runtime counts reject zero, missing, ignored and substituted native witnesses", () => {
  for (const platform of ["win32", "darwin"]) for (const recipe of recipes(platform).filter(recipe => !recipe.actor)) {
    const names = cases(recipe), log = `test result: ok. ${names.length} passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s`;
    const list = [...names, "unselected::other"].map(name => name + ": test").join("\r\n") + "\r\n";
    if (recipe.filters.length) assert.deepEqual(selectedCases(list, recipe), [...names].sort());
    assertRuntime(log, names, recipe);
    assert.throws(() => assertRuntime("", names, recipe));
    assert.throws(() => assertRuntime(log.replace("0 ignored", "1 ignored"), names, recipe));
    assert.throws(() => assertRuntime(log.replace("0 failed", "1 failed"), names, recipe));
    assert.throws(() => assertRuntime(log, names.slice(1), recipe));
    if (recipe.recorder) assert.throws(() => assertRuntime(log, names.map(name => name === names[0] ? "other::substituted" : name), recipe));
  }
});

test("compatible repeated Cargo units require real freshness while different features/profile/frontend/toolchains remain distinct", () => {
  const unit = { package_id: "server", target: { name: "bamboo_server", kind: ["lib"] }, features: [],
    profile: { test: true, opt_level: "0" }, filenames: ["owned/server.exe"], fresh: false };
  const seen = new Map(), context = { id: "first", toolchain: "stable", rustc: "rustc 1.99", frontend: "embedded" };
  observeArtifacts([unit], context, seen, "server");
  assert.throws(() => observeArtifacts([unit], context, seen, "server"), /compiled again/);
  assert.equal(observeArtifacts([{ ...unit, fresh: true }], { ...context, id: "next" }, seen, "server")[0].reusedFrom, "first");
  observeArtifacts([{ ...unit, features: ["test-utils"] }], context, seen, "server");
  observeArtifacts([{ ...unit, profile: { test: true, opt_level: "z" } }], context, seen, "server");
  observeArtifacts([unit], { ...context, frontend: "api-only" }, seen, "server");
  observeArtifacts([unit], { ...context, toolchain: ACTOR_TOOLCHAIN }, seen, "server");
  assert.throws(() => observeArtifacts([{ ...unit, fresh: undefined }], context, seen, "server"), /freshness/);
});

test("Actor runtime requires original scope receipts rather than a green compile wrapper", () => {
  const manifest = { scopes: actorScopes };
  const text = actorScopes.map(scope => `${scope.filter} : ${scope.names.length} passed; zero failed/ignored`).join("\n");
  assertActorCounts(text, manifest);
  assert.throws(() => assertActorCounts("", manifest));
  assert.throws(() => assertActorCounts(text.replace("9 passed", "8 passed"), manifest));
});

function fixture(t, platform, fault) {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "bamboo-native-fixture-")));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const temporary = path.join(root, "temporary"); fs.mkdirSync(temporary);
  const target = path.join(root, "target/native-feature");
  const revision = "a".repeat(40), tree = "b".repeat(40), calls = [], lists = new Map(), counted = new Set();
  const names = [...new Set([...recipes(platform).map(recipe => recipe.package).filter(Boolean), "bamboo-storage", "bamboo-tools", "bamboo-server"])];
  const packages = names.map(name => {
    const directory = path.join(root, "crates", name); fs.mkdirSync(directory, { recursive: true });
    const manifest = path.join(directory, "Cargo.toml"); fs.writeFileSync(manifest, "fixture");
    return { name, id: "path+file://" + directory + "#0.0.0", manifest_path: manifest, features: {} };
  });
  function artifact(owner, name, kind, profile, names, label) {
    const executable = path.join(target, label + "-" + name + (platform === "win32" ? ".exe" : ""));
    fs.mkdirSync(path.dirname(executable), { recursive: true }); fs.writeFileSync(executable, label + name);
    lists.set(executable, names.map(name => name + ": test").join("\n"));
    return { reason: "compiler-artifact", package_id: owner.id, manifest_path: owner.manifest_path,
      target: { name, kind: [kind] }, features: [], profile, executable, filenames: [executable], fresh: false };
  }
  function dependency() {
    const unit = { reason: "compiler-artifact", package_id: "registry+common", target: { name: "common", kind: ["lib"] }, features: [],
      profile: { test: false, opt_level: "0" }, filenames: [path.join(target, "common.rlib")], fresh: counted.has("common") };
    counted.add("common"); return unit;
  }
  const execute = (program, args, options) => {
    calls.push({ program, args, env: options.env, cwd: options.cwd, timeout: options.timeout });
    let stdout = "";
    if (program === "git") stdout = args[0] === "rev-parse" ? (args[1] === "HEAD" ? revision : tree) : fault === "dirty" ? " M source" : "";
    else if (program === "rustc") stdout = "rustc 1.99.0\nhost: " + (platform === "win32" ? "x86_64-pc-windows-msvc" : "aarch64-apple-darwin");
    else if (program === "cargo" && args[0] === "-V") stdout = "cargo 1.99.0";
    else if (program === "cargo" && args[0] === "metadata") stdout = JSON.stringify({ packages });
    else if (program === "pwsh") {
      const evidence = args[args.indexOf("-Evidence") + 1];
      if (args.includes("-RecordHost")) fs.mkdirSync(evidence, { recursive: true });
      if (args.includes("-CompileAcceptance")) {
        const records = {};
        for (const name of ["bamboo-storage", "bamboo-server", "bamboo-tools"]) {
          const owner = packages.find(owner => owner.name === name);
          const unit = artifact(owner, name.replaceAll("-", "_"), "lib", { test: true, opt_level: "0" }, [], "actor");
          records[name] = { package: name, snapshot: unit.executable, sha256: digest(unit.executable).toUpperCase(), features: fault === "storage-leak" && name === "bamboo-storage" ? ["test-utils"] : [], profile: unit.profile };
          fs.writeFileSync(path.join(evidence, "compile-" + name + ".jsonl"), [dependency(), unit].map(unit => JSON.stringify(unit)).join("\n"));
        }
        fs.writeFileSync(path.join(evidence, "verified-binaries.json"), JSON.stringify({ schemaVersion: 1, workspace: root, targetRoot: target,
          revision, tree, packages: records, scopes: actorScopes }));
      }
      if (args.includes("-ExecuteAcceptance") && fault !== "actor-runtime") fs.writeFileSync(path.join(evidence, "verified-counts.txt"), actorScopes.map(scope => `${scope.filter} : ${scope.names.length} passed; zero failed/ignored`).join("\n"));
    } else if (program === "cargo") {
      const recipe = recipes(platform).find(recipe => recipe.package === args[args.indexOf("-p") + 1] && Boolean(recipe.unwind) === args.includes("--profile"));
      const owner = packages.find(owner => owner.name === recipe.package), selected = cases(recipe);
      const profile = { test: true, opt_level: recipe.unwind ? "z" : "0" };
      const units = [dependency()];
      if (recipe.id === "server-default") {
        units.push(artifact(owner, "bamboo_server", "lib", profile, selected.slice(0, 7), recipe.id));
        units.push(artifact(owner, "frontend_build_contract", "test", profile, selected.slice(7), recipe.id));
      } else units.push(artifact(owner, recipe.package.replaceAll("-", "_"), recipe.targets.includes("--test") || recipe.recorder ? "test" : "lib", profile, selected, recipe.id));
      if (recipe.recorder && fault !== "recorder-binary") units.push(artifact(owner, "tool-event-recorder", "bin", { test: false, opt_level: "0" }, [], recipe.id));
      if (fault === "recompile" && recipe.id === "skills-default") units[0].fresh = false;
      stdout = units.map(unit => JSON.stringify(unit)).join("\n") + `\ntest result: ok. ${selected.length} passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s\n`;
      if (fault === "zero-summary" && recipe.recorder) stdout = stdout.replace(/test result:[^\n]+/u, "");
      if (fault === "unwind-warning" && recipe.unwind) stdout += "__eh_frame section too large\n";
    } else if (lists.has(program)) stdout = lists.get(program);
    else throw Error("Unexpected fixture command: " + program);
    return { status: 0, stdout, stderr: "" };
  };
  return { root, target, temporary, revision, tree, calls, execute };
}

for (const [platform, expected] of [["win32", 145], ["darwin", 107]]) test(`fixed ${platform} owner proves ${expected} named cases in one target with real freshness bindings`, t => {
  const f = fixture(t, platform);
  const result = runNativeOwner({ root: f.root, platform, execute: f.execute, environment: { RUNNER_TEMP: f.temporary, GITHUB_SHA: f.revision,
    GITHUB_TOKEN: "private", BAMBOO_RELEASE_SIGNING_KEY: "private", HTTP_PROXY: "private" } });
  assert.equal(result.status, 0);
  assert.equal(result.contexts.reduce((total, context) => total + context.cases.length, 0), expected);
  assert.ok(f.calls.every(call => call.env.CARGO_TARGET_DIR === f.target));
  assert.ok(f.calls.every(call => !Object.keys(call.env).some(key => /TOKEN|SIGNING_KEY|PROXY/u.test(key))));
  assert.ok(result.contexts.some(context => context.units.some(unit => unit.reusedFrom && unit.fresh)));
  assert.equal(new Set(f.calls.filter(call => call.program === "cargo" && call.args[0] === "test").map(call => call.args.join(" "))).size,
    f.calls.filter(call => call.program === "cargo" && call.args[0] === "test").length, "Do not invoke the same native Cargo recipe twice");
  assert.equal(JSON.parse(fs.readFileSync(path.join(result.directory, "native-owner.json"))).status, 0);
  if (platform === "win32") {
    for (const [action, minutes] of [["-CompileAcceptance", 20], ["-ExecuteAcceptance", 5]]) {
      assert.equal(f.calls.find(call => call.args.includes(action)).timeout, minutes * 60 * 1000, "Retain original Actor compile/runtime budgets");
    }
    assert.equal(f.calls.find(call => call.args.includes("scripts/actor-windows-native.test.ps1")).timeout, 60 * 1000);
  }
});

for (const fault of ["actor-runtime", "storage-leak", "recorder-binary", "zero-summary", "recompile"]) test("native owner fails closed on " + fault, t => {
  const f = fixture(t, "win32", fault);
  const result = runNativeOwner({ root: f.root, platform: "win32", execute: f.execute, environment: { RUNNER_TEMP: f.temporary } });
  assert.equal(result.status, 1);
  assert.ok(result.contexts.some(context => context.status === 0), "Preserve independent native outcomes after a selected failure");
  assert.ok(result.contexts.some(context => context.error));
});

test("macOS unwind diagnostics fail acceptance even when Cargo returns zero", t => {
  const f = fixture(t, "darwin", "unwind-warning");
  const result = runNativeOwner({ root: f.root, platform: "darwin", execute: f.execute, environment: { RUNNER_TEMP: f.temporary } });
  assert.equal(result.status, 1);
  assert.match(result.contexts.find(context => context.id === "macos-unwind").error, /oversized unwind/);
  assert.ok(result.contexts.slice(1).every(context => context.status === 0));
});

test("native source and executable guards reject dirty source and escaped/tampered files", t => {
  const f = fixture(t, "win32", "dirty");
  assert.throws(() => runNativeOwner({ root: f.root, platform: "win32", execute: f.execute, environment: { RUNNER_TEMP: f.temporary } }), /dirty/);
  assert.ok(!f.calls.some(call => call.program === "cargo"));
  const file = path.join(f.root, "outside.exe"); fs.writeFileSync(file, "outside");
  assert.throws(() => fileIdentity(file, f.target), /escaped/);
});
