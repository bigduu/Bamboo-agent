#!/usr/bin/env node
"use strict";

// Fixed native acceptance recipes. Each OS owns one workspace/target; Cargo
// retains distinct package feature graphs, profiles and frontend build modes.
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const os = require("node:os");
const { createHash, randomBytes } = require("node:crypto");
const { spawnSync } = require("node:child_process");

const SKILLS = ["progressive::source_tests::", "progressive::read_tests::"];
const TLS = "server::tls::tests::";
const FRONTEND = "frontend_build::tests::";
const TICKET = "workspace_files::physical::tests::unsupported_platform_file_operations_fail_closed_without_mutation";
const ACTOR_TOOLCHAIN = "1.99.0-x86_64-pc-windows-msvc";

function recipes(platform) {
  assert.ok(["win32", "darwin"].includes(platform), "Native ownership requires Windows or macOS");
  const windows = platform === "win32";
  return [
    ...(windows ? [{ id: "actor", actor: true, toolchain: ACTOR_TOOLCHAIN, frontend: "api-only" },
      { id: "server-default", package: "bamboo-server", targets: ["--lib", "--test", "frontend_build_contract"],
        filters: [TLS, FRONTEND], count: 14 }]
      : [{ id: "macos-unwind", package: "bamboo-server", targets: ["--lib"], filters: [TLS], count: 8, unwind: true },
        { id: "frontend-default", package: "bamboo-server", targets: ["--test", "frontend_build_contract"], filters: [], count: 7 }]),
    { id: "recorder-default", package: "bamboo-tool-event-recorder-example", targets: [], filters: [], count: 4, recorder: true },
    ...(windows ? [{ id: "ticket-default", package: "bamboo-tickets", targets: ["--lib"], filters: [TICKET], count: 1, exact: true }] : []),
    { id: "skills-default", package: "bamboo-skills", targets: ["--lib"], filters: SKILLS, count: windows ? 39 : 41 },
    { id: "skill-tool-default", package: "bamboo-server-tools", targets: ["--lib"], filters: ["skill_runtime::catalog_tests::"], count: windows ? 46 : 47 },
  ].map(recipe => ({ toolchain: "stable", frontend: "embedded", profile: recipe.unwind ? "dev" : "test", ...recipe }));
}

function childEnvironment(environment, target, recipe) {
  const result = Object.fromEntries(Object.entries(environment).filter(([key]) =>
    !/TOKEN|SECRET|PASSWORD|KEY|AUTH|CREDENTIAL|TUNNEL|PROXY|COOKIE/i.test(key)
    && !/^GITHUB_(?:ENV|OUTPUT|PATH|STATE|STEP_SUMMARY)$/u.test(key)));
  result.CARGO_TARGET_DIR = target;
  result.RUSTUP_TOOLCHAIN = recipe.toolchain;
  if (recipe.frontend === "api-only") result.BAMBOO_FRONTEND_BUILD_MODE = "api-only";
  else delete result.BAMBOO_FRONTEND_BUILD_MODE;
  return result;
}

function inside(root, file) {
  const relative = path.relative(root, file);
  assert.ok(relative && !path.isAbsolute(relative) && relative.split(path.sep)[0] !== "..", "Native output escaped its owned target");
  return file;
}

function fileIdentity(file, target) {
  inside(target, path.resolve(file));
  const stat = fs.lstatSync(file);
  assert.ok(stat.isFile() && !stat.isSymbolicLink(), "Native executable must be a regular file");
  const normalize = value => process.platform === "win32" ? value.toLowerCase() : value;
  assert.equal(normalize(fs.realpathSync(file)), normalize(path.resolve(file)), "Native executable traverses a link");
  const hash = createHash("sha256"), buffer = Buffer.alloc(1024 * 1024);
  const descriptor = fs.openSync(file, "r");
  try { let count; while ((count = fs.readSync(descriptor, buffer, 0, buffer.length, null))) hash.update(buffer.subarray(0, count)); }
  finally { fs.closeSync(descriptor); }
  return { path: file, sha256: hash.digest("hex"), sizeBytes: stat.size };
}

function compilerArtifacts(log) {
  return log.split(/\r?\n/u).flatMap(line => {
    try { const value = JSON.parse(line); return value.reason === "compiler-artifact" ? [value] : []; }
    catch { return []; }
  });
}

function selectedCases(list, recipe) {
  const names = [...list.matchAll(/^(.+): test\r?$/gmu)].map(match => match[1]).sort();
  assert.equal(names.length, new Set(names).size, "Native catalog contains duplicate names");
  return names.filter(name => !recipe.filters.length || recipe.filters.some(filter => recipe.exact ? name === filter : name.includes(filter)));
}

function assertRuntime(log, names, recipe) {
  assert.equal(names.length, recipe.count, "Native named-case inventory changed: " + recipe.id);
  const summaries = [...log.matchAll(/test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored;/gu)];
  assert.ok(summaries.length, "Native runtime has no test summary");
  assert.ok(summaries.every(match => match[1] === "ok" && match[3] === "0" && match[4] === "0"), "Native runtime failed or ignored required cases");
  assert.equal(summaries.reduce((total, match) => total + Number(match[2]), 0), names.length, "Native runtime did not execute the enumerated selection");
  const mandatory = recipe.id === "skills-default" ? (recipe.count === 39
    ? ["windows_real_opened_handle_identity_admits_and_reuses_regular_sources", "windows_real_directory_junction_cannot_supply_instruction_policy", "actual_windows_junction_is_created_and_rejected_without_reading_target"]
    : ["file_and_directory_links_and_fifo_cannot_supply_policy_or_main", "unix_file_directory_link_fifo_and_read_error_do_not_become_empty_or_absent"])
    : recipe.id === "skill-tool-default" && recipe.count === 46 ? ["skills_read_native_windows_junction_replacement_rejects_foreign_auxiliary_data"]
      : recipe.recorder ? ["native_recorder_lifecycle_is_bounded_and_generation_safe", "boot_reconcile_restarts_the_installed_native_recorder"]
        : recipe.unwind ? ["macos_test_link_keeps_dwarf_backtraces"] : [];
  for (const suffix of mandatory) assert.ok(names.some(name => name.endsWith(suffix)), "Native case is missing: " + suffix);
}

function cargoArguments(recipe) {
  return ["test", "--locked", "--no-fail-fast", "--message-format=json-render-diagnostics", "-p", recipe.package,
    ...(recipe.unwind ? ["--profile", "dev", "--all-features"] : []), ...recipe.targets,
    "--", ...recipe.filters, ...(recipe.exact ? ["--exact"] : []), "--nocapture", "--test-threads=1", "--color=never"];
}

function observeArtifacts(units, context, seen, serverId) {
  return units.map(unit => {
    assert.equal(typeof unit.fresh, "boolean", "Cargo must report actual artifact freshness");
    const key = JSON.stringify([unit.package_id, unit.target.name, unit.target.kind, unit.features, unit.profile,
      unit.filenames, context.toolchain, context.rustc, unit.package_id === serverId ? context.frontend : null]);
    assert.ok(!seen.has(key) || unit.fresh, "Compatible Cargo unit was compiled again: " + unit.target.name);
    const reusedFrom = seen.get(key) || null;
    seen.set(key, context.id);
    return { packageId: unit.package_id, target: unit.target, features: unit.features, profile: unit.profile,
      fresh: unit.fresh, filenames: unit.filenames, reusedFrom };
  });
}

function assertActorManifest(manifest, root, target, source) {
  assert.equal(manifest.schemaVersion, 1);
  assert.equal(manifest.revision, source.revision);
  assert.equal(manifest.tree, source.tree);
  assert.equal(path.resolve(manifest.workspace), root);
  assert.equal(path.resolve(manifest.targetRoot), target);
  assert.deepEqual(Object.keys(manifest.packages).sort(), ["bamboo-server", "bamboo-storage", "bamboo-tools"]);
  assert.equal(manifest.scopes.length, 5);
  const names = manifest.scopes.flatMap(scope => scope.names.map(name => scope.package + "/" + name));
  assert.equal(names.length, 41); assert.equal(new Set(names).size, 41);
  for (const record of Object.values(manifest.packages)) {
    assert.equal(fileIdentity(record.snapshot, target).sha256.toUpperCase(), record.sha256);
    assert.ok(record.profile.test && record.profile.opt_level === "0", "Actor must retain its ordinary test profile");
  }
  const storage = manifest.packages["bamboo-storage"].features;
  assert.ok(!storage.includes("test-utils"), "Standalone Actor storage cannot acquire server test-utils");
  return names;
}

function assertActorCounts(text, manifest) {
  const records = [...text.matchAll(/^(.+) : (\d+) passed; zero failed\/ignored\r?$/gmu)].map(match => [match[1], Number(match[2])]);
  assert.equal(records.length, 5, "Actor runtime did not receipt all five scopes");
  assert.deepEqual(records.sort(), manifest.scopes.map(scope => [scope.filter, scope.names.length]).sort(), "Actor runtime counts differ from the frozen named manifest");
}

function runNativeOwner({ root = process.cwd(), environment = process.env, platform = process.platform, execute = spawnSync } = {}) {
  root = fs.realpathSync(root);
  const target = path.resolve(environment.CARGO_TARGET_DIR || path.join(root, "target/native-feature"));
  inside(root, target); fs.mkdirSync(target, { recursive: true });
  assert.equal(fs.realpathSync(target), target, "Owned native target cannot be a link");
  const directory = fs.mkdtempSync(path.join(environment.RUNNER_TEMP || os.tmpdir(), "bamboo-native-owner-"));
  const contexts = [], commands = [], seen = new Map();
  function run(program, args, env, cwd, label, timeout = 50 * 60 * 1000) {
    const logFile = path.join(directory, label + ".log"), descriptor = fs.openSync(logFile, "wx");
    // Native commands write while running: a killed parent must retain their
    // partial evidence. Injected unit fixtures return structured stdout instead.
    const streaming = execute === spawnSync && (program === "pwsh" || program === "cargo" && args[0] === "test");
    let result;
    try { result = execute(program, args, { cwd, env, encoding: "utf8", maxBuffer: 64 * 1024 * 1024,
      timeout, ...(streaming ? { stdio: ["ignore", descriptor, descriptor] } : {}) }); }
    finally { fs.closeSync(descriptor); }
    if (!streaming) fs.appendFileSync(logFile, `${result.stdout || ""}${result.stderr || ""}`);
    assert.ok(fs.statSync(logFile).size <= 64 * 1024 * 1024, "Native evidence exceeds its bound");
    const log = fs.readFileSync(logFile, "utf8");
    commands.push({ label, program, args, timeoutMs: timeout, status: result.error ? 1 : result.status ?? 1 });
    if (environment.GITHUB_ACTIONS) {
      const stop = randomBytes(24).toString("hex");
      process.stdout.write(`::stop-commands::${stop}\n${log}\n::${stop}::\n`);
    }
    assert.ok(!result.error && result.status === 0, "Native command failed: " + label);
    return streaming || program === "cargo" && args[0] === "test" ? log : result.stdout || "";
  }
  const defaultEnv = childEnvironment(environment, target, { toolchain: "stable", frontend: "embedded" });
  const revision = run("git", ["rev-parse", "HEAD"], defaultEnv, root, "source-sha").trim();
  const tree = run("git", ["rev-parse", "HEAD^{tree}"], defaultEnv, root, "source-tree").trim();
  assert.match(revision, /^[a-f0-9]{40}$/u); assert.match(tree, /^[a-f0-9]{40}$/u);
  if (environment.GITHUB_SHA) assert.equal(revision, environment.GITHUB_SHA);
  const clean = label => assert.equal(run("git", ["status", "--porcelain", "--untracked-files=all"], defaultEnv, root, label).trim(), "", "Native source is dirty");
  clean("source-clean-before");
  const metadata = JSON.parse(run("cargo", ["metadata", "--format-version", "1", "--no-deps", "--locked"], defaultEnv, root, "metadata"));
  const serverId = metadata.packages.find(owner => owner.name === "bamboo-server")?.id;
  assert.ok(serverId, "Missing native server package");
  const source = { revision, tree };
  for (const recipe of recipes(platform)) {
    const env = childEnvironment(environment, target, recipe);
    const context = { ...recipe, source, targetDirectory: target, status: 1 };
    contexts.push(context);
    try {
      context.rustc = run("rustc", ["-vV"], env, root, recipe.id + "-rustc").trim();
      context.cargo = run("cargo", ["-V"], env, root, recipe.id + "-cargo").trim();
      if (recipe.actor) {
        const evidence = path.join(directory, "actor");
        run("pwsh", ["-NoLogo", "-NoProfile", "-File", "scripts/actor-windows-native.test.ps1"], env, root, "actor-controller-tests", 60 * 1000);
        for (const [action, minutes] of [["RecordHost", 5], ["RecordToolchain", 5], ["CompileAcceptance", 20], ["ExecuteAcceptance", 5]]) {
          run("pwsh", ["-NoLogo", "-NoProfile", "-File", "scripts/actor-windows-native.ps1", "-" + action, "-Workspace", root, "-Evidence", evidence], env, root, "actor-" + action, minutes * 60 * 1000);
        }
        const manifest = JSON.parse(fs.readFileSync(path.join(evidence, "verified-binaries.json"), "utf8"));
        context.cases = assertActorManifest(manifest, root, target, source);
        assertActorCounts(fs.readFileSync(path.join(evidence, "verified-counts.txt"), "utf8"), manifest);
        context.units = ["bamboo-storage", "bamboo-server", "bamboo-tools"].flatMap(owner => {
          const units = compilerArtifacts(fs.readFileSync(path.join(evidence, "compile-" + owner + ".jsonl"), "utf8"));
          assert.ok(units.length, "Actor compilation has no compiler-artifact evidence");
          return observeArtifacts(units, { ...context, id: "actor-" + owner }, seen, serverId);
        });
        context.status = 0;
        continue;
      }
      const owner = metadata.packages.find(owner => owner.name === recipe.package);
      assert.ok(owner, "Missing native package: " + recipe.package);
      if (recipe.recorder) assert.equal(Object.keys(owner.features).length, 0, "Recorder feature changes require explicit native ownership review");
      const args = cargoArguments(recipe);
      const log = run("cargo", args, env, root, recipe.id + "-run");
      if (recipe.unwind) assert.ok(!log.includes("__eh_frame section too large"), "macOS native lib-test emitted oversized unwind diagnostics");
      context.command = ["cargo", ...args];
      const units = compilerArtifacts(log);
      context.units = observeArtifacts(units, context, seen, serverId);
      const tests = units.filter(unit => unit.package_id === owner.id && unit.profile.test && unit.executable);
      assert.ok(tests.length, "Native command reported no owned test executable");
      const names = [], harnesses = new Set();
      context.executables = [];
      for (const unit of tests) {
        assert.equal(unit.profile.opt_level, recipe.unwind ? "z" : "0", "Native test profile differs from its owned configuration");
        const before = fileIdentity(unit.executable, target);
        const key = unit.target.kind.join(",") + "/" + unit.target.name;
        assert.ok(!harnesses.has(key), "Ambiguous native test executable"); harnesses.add(key);
        const cwd = path.dirname(owner.manifest_path);
        const list = run(unit.executable, ["--list", "--format", "terse"], { ...env, CARGO_MANIFEST_DIR: cwd }, cwd, recipe.id + "-list-" + harnesses.size);
        assert.deepEqual(fileIdentity(unit.executable, target), before, "Native executable changed while listing");
        names.push(...selectedCases(list, recipe));
        context.executables.push({ ...before, target: unit.target, features: unit.features, profile: unit.profile });
      }
      assert.equal(names.length, new Set(names).size, "Native case has more than one runtime owner within its configuration");
      assertRuntime(log, names, recipe);
      if (recipe.recorder) {
        const bins = units.filter(unit => unit.package_id === owner.id && unit.target.name === "tool-event-recorder" && !unit.profile.test && unit.executable);
        assert.equal(bins.length, 1, "Recorder E2E requires the Cargo-built CARGO_BIN_EXE_tool-event-recorder");
        context.recorderBinary = fileIdentity(bins[0].executable, target);
      }
      context.cases = names.sort(); context.status = 0;
    } catch (error) { context.error = error.message; process.stderr.write(recipe.id + ": " + error.message + "\n"); }
  }
  try { clean("source-clean-after"); } catch (error) { contexts.push({ id: "source", status: 1, error: error.message }); }
  const status = contexts.some(context => context.status !== 0) ? 1 : 0;
  const result = { schemaVersion: 1, source, platform, targetDirectory: target, status, contexts, commands };
  fs.writeFileSync(path.join(directory, "native-owner.json"), JSON.stringify(result, null, 2) + "\n", { flag: "wx" });
  if (environment.GITHUB_STEP_SUMMARY) fs.appendFileSync(environment.GITHUB_STEP_SUMMARY,
    `Native ownership (${platform}): ${contexts.reduce((count, context) => count + (context.cases?.length || 0), 0)} named cases; status=${status}; one target=${target}.\n`);
  return { ...result, directory };
}

module.exports = { ACTOR_TOOLCHAIN, TLS, FRONTEND, TICKET, recipes, childEnvironment, fileIdentity, compilerArtifacts, selectedCases, assertRuntime, cargoArguments, observeArtifacts, assertActorManifest, assertActorCounts, runNativeOwner };
if (require.main === module) {
  try { process.exitCode = runNativeOwner().status; }
  catch (error) { console.error(error.message); process.exitCode = 1; }
}
