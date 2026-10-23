#!/usr/bin/env node
'use strict';

// Local libtest controller. No dependencies, CI activation, or cross-SHA reuse.
// The Cargo-JSON/hash/list/count contract follows actor-windows-native.ps1;
// that runner's separate 41-case Windows acceptance contract remains intact.
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const crypto = require('node:crypto');
const { spawn, spawnSync } = require('node:child_process');
const CONFIG = require('./test-groups.json');
const ROOT = path.resolve(__dirname, '..');
const fail = message => { throw new Error(message); };
const canonical = value => JSON.stringify(value, (_, v) => v && typeof v === 'object' && !Array.isArray(v)
  ? Object.fromEntries(Object.keys(v).sort().map(k => [k, v[k]])) : v);
const digest = value => crypto.createHash('sha256').update(canonical(value)).digest('hex');
const fileHash = filename => {
  const stat = fs.lstatSync(filename);
  if (!stat.isFile() || stat.isSymbolicLink()) fail(`Not a regular artifact: ${filename}`);
  const hash = crypto.createHash('sha256');
  const fd = fs.openSync(filename, 'r');
  try {
    const buffer = Buffer.alloc(1024 * 1024);
    for (let size; (size = fs.readSync(fd, buffer, 0, buffer.length, null));) hash.update(buffer.subarray(0, size));
  } finally { fs.closeSync(fd); }
  return hash.digest('hex');
};
const writeJSON = (filename, value) => fs.writeFileSync(filename, JSON.stringify(value, null, 2) + '\n', { flag: 'wx' });
const readJSON = filename => JSON.parse(fs.readFileSync(filename, 'utf8'));
// A process hashes each artifact once, then rehashes only if its physical/stat
// identity changes. Every new controller process verifies all bytes again.
const verifiedArtifacts = new Map();
function verifyArtifact(artifact) {
  const stat = fs.lstatSync(artifact.path, { bigint: true });
  const key = [stat.dev, stat.ino, stat.mode, stat.size, stat.mtimeNs, stat.ctimeNs].join(':');
  const cached = verifiedArtifacts.get(artifact.path);
  if (!cached || cached.key !== key || cached.hash !== artifact.sha256) {
    if (fileHash(artifact.path) !== artifact.sha256) fail(`Artifact bytes changed: ${artifact.path}`);
    verifiedArtifacts.set(artifact.path, { key, hash: artifact.sha256 });
  }
}
function syncText(command, args, cwd) {
  const result = spawnSync(command, args, { cwd, encoding: 'utf8', timeout: 30000, maxBuffer: 16 * 1024 * 1024 });
  if (result.error || result.status !== 0) fail(`Cannot inspect ${command}: ${result.error?.message || result.stderr}`);
  return result.stdout.trim();
}
function identity(root, config = CONFIG) {
  root = fs.realpathSync(root);
  if (syncText('git', ['status', '--porcelain', '--untracked-files=all'], root)) fail('Source must be clean, including untracked files');
  const environment = Object.fromEntries(Object.entries(process.env)
    .filter(([key]) => !['_', 'PWD', 'OLDPWD', 'SHLVL'].includes(key)).sort(([a], [b]) => a.localeCompare(b)));
  const cargoHomes = [path.resolve(root, process.env.CARGO_HOME || path.join(os.homedir(), '.cargo'))];
  for (let directory = root; ; directory = path.dirname(directory)) {
    cargoHomes.push(path.join(directory, '.cargo'));
    if (directory === path.dirname(directory)) break;
  }
  const cargoConfig = [...new Set(cargoHomes.flatMap(directory => ['config', 'config.toml'].map(name => path.join(directory, name))))]
    .sort().map(filename => ({ path: filename, sha256: fs.existsSync(filename) ? fileHash(fs.realpathSync(filename)) : null }));
  return {
    root, sha: syncText('git', ['rev-parse', 'HEAD'], root), tree: syncText('git', ['rev-parse', 'HEAD^{tree}'], root),
    platform: process.platform, arch: process.arch, osRelease: os.release(), hostHash: digest(os.hostname()),
    node: process.version, environmentHash: digest(environment), cargoConfig, configHash: digest(config), runnerHash: fileHash(__filename),
  };
}
function validateConfig(config) {
  if (config.schemaVersion !== 1 || !config.groups[config.fallbackGroup]) fail('Invalid test group configuration');
  for (const [name, group] of Object.entries(config.groups)) {
    if (!/^[a-z][a-z0-9-]*$/.test(name) || !(group.timeoutSeconds > 0)) fail(`Invalid group: ${name}`);
  }
  for (const rule of config.routes) {
    if (!config.groups[rule.group] || !Number.isInteger(rule.priority)) fail('Invalid routing rule');
    for (const key of ['targetRegex', 'testRegex']) if (rule[key]) new RegExp(rule[key]);
  }
}
function route(record, name, config = CONFIG) {
  const matches = config.routes.filter(r => (!r.package || r.package === record.package) &&
    (!r.kind || record.kind.includes(r.kind)) && (!r.targetRegex || new RegExp(r.targetRegex).test(record.target)) &&
    (!r.testRegex || new RegExp(r.testRegex).test(name)));
  const priority = Math.min(...matches.map(r => r.priority));
  const winners = new Set(matches.filter(r => r.priority === priority).map(r => r.group));
  if (winners.size > 1) fail(`Ambiguous routing: ${record.package}/${record.target}/${name}`);
  return winners.size ? [...winners][0] : config.fallbackGroup;
}
function parseList(output) {
  const names = [], benchmarks = [], summaries = [];
  for (const line of output.split(/\r?\n/).filter(Boolean)) {
    let match;
    if ((match = line.match(/^(.+): (test|benchmark)$/))) (match[2] === 'test' ? names : benchmarks).push(match[1]);
    else if ((match = line.match(/^(\d+) tests?, (\d+) benchmarks?$/))) summaries.push(match.slice(1).map(Number));
    else fail(`Unexpected libtest --list output: ${line}`);
  }
  if (summaries.length !== 1 || summaries[0][0] !== names.length || summaries[0][1] !== benchmarks.length ||
      new Set([...names, ...benchmarks]).size !== names.length + benchmarks.length) fail('Invalid, duplicate or incomplete --list counts');
  if (benchmarks.length) fail('Benchmark entries need a separate lane; cannot count them as tests');
  return names.sort();
}
function equalNames(actual, expected) {
  if (canonical([...actual].sort()) !== canonical([...expected].sort())) fail('Selected --list names differ from the frozen selection');
}
function parseCounts(output, expected, ignored) {
  const lines = output.split(/\r?\n/).filter(line => /^test result:/.test(line));
  const match = lines.length === 1 && lines[0].match(/^test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out; finished in .+$/);
  if (!match) fail('Missing or duplicate libtest result summary');
  const [passed, failed, skipped, measured, filtered] = match.slice(2).map(Number);
  if (passed + failed + skipped !== expected || skipped !== ignored || measured !== 0 ||
      ((failed === 0) !== (match[1] === 'ok'))) fail('Runtime counts differ from the frozen selection/ignored semantics');
  return { passed, failed, ignored: skipped, measured, filtered };
}

// Use the shortest pure namespace filter, verified with --list before execution.
// Substring collisions fall back to one --exact test, without skip-list or ARG_MAX assumptions.
function partition(record, names) {
  if (names.length === record.tests.length) return [{ args: [], names }];
  const selected = new Set(names), covered = new Set(), commands = [];
  for (const name of [...names].sort()) {
    if (covered.has(name)) continue;
    const parts = name.split('::');
    let command;
    for (let length = 1; length < parts.length; length++) {
      const filter = parts.slice(0, length).join('::') + '::';
      const found = record.tests.map(t => t.name).filter(n => n.includes(filter));
      if (found.every(n => selected.has(n) && !covered.has(n))) { command = { args: [filter], names: found.sort() }; break; }
    }
    command ||= { args: [name, '--exact'], names: [name] };
    command.names.forEach(n => covered.add(n)); commands.push(command);
  }
  equalNames([...covered], names);
  return commands;
}
function plan(manifest, ignoredMode = 'default', config = CONFIG) {
  if (!['default', 'only', 'include'].includes(ignoredMode)) fail('Unknown ignored mode');
  const groups = Object.fromEntries(Object.keys(config.groups).map(name => [name, []]));
  const seen = new Set();
  for (const record of manifest.targets) {
    const selected = {};
    for (const test of record.tests) {
      const key = `${record.id}\0${test.name}`;
      if (seen.has(key)) fail(`Duplicate inventory test: ${key}`);
      seen.add(key);
      const group = route(record, test.name, config);
      if (ignoredMode !== 'only' || test.ignored) (selected[group] ||= []).push(test.name);
    }
    for (const [group, names] of Object.entries(selected)) {
      for (const command of partition(record, names)) {
        const ignored = ignoredMode === 'default' ? record.tests.filter(t => t.ignored && command.names.includes(t.name)).length : 0;
        const flags = ignoredMode === 'only' ? ['--ignored'] : ignoredMode === 'include' ? ['--include-ignored'] : [];
        groups[group].push({ targetId: record.id, args: [...command.args, ...flags], names: command.names, ignored });
      }
    }
  }
  if (!seen.size) fail('No tests in compiled profile');
  const expected = manifest.targets.flatMap(t => t.tests.filter(test => ignoredMode !== 'only' || test.ignored).map(test => `${t.id}\0${test.name}`));
  const assigned = Object.values(groups).flat().flatMap(c => c.names.map(n => `${c.targetId}\0${n}`));
  equalNames(assigned, expected);
  if (new Set(assigned).size !== assigned.length) fail('Duplicate group selection');
  return groups;
}
function selectGroups(groups, requested, all) {
  if (all && requested.length) fail('Choose --all or --group, not both');
  if (!all && !requested.length) fail('Select --group NAME or --all explicitly');
  if (new Set(requested).size !== requested.length) fail('Duplicate requested group');
  const selected = all ? Object.keys(groups).filter(g => groups[g].length) : requested;
  for (const group of selected) {
    if (!Object.hasOwn(groups, group)) fail(`Unknown group: ${group}`);
    if (!groups[group].length) fail(`Group has zero matching tests in this profile/ignored mode: ${group}`);
  }
  if (!selected.length) fail('Zero matching tests');
  return selected;
}

async function processRun(command, args, { cwd, env = process.env, timeoutMs, log }) {
  const stdoutFd = fs.openSync(log + '.stdout.log', 'wx'), stderrFd = fs.openSync(log + '.stderr.log', 'wx');
  const started = Date.now(), output = { stdout: '', stderr: '' };
  let timedOut = false, interrupted = false, error = null, bytes = 0;
  return new Promise(resolve => {
    const child = spawn(command, args, { cwd, env, detached: process.platform !== 'win32', stdio: ['ignore', 'pipe', 'pipe'] });
    const kill = () => {
      if (!child.pid) return;
      try {
        if (process.platform === 'win32') spawnSync('taskkill', ['/PID', String(child.pid), '/T', '/F'], { timeout: 10000, windowsHide: true });
        else process.kill(-child.pid, 'SIGKILL');
      } catch (e) { if (e.code !== 'ESRCH') error = e.message; }
    };
    const interrupt = () => { interrupted = true; kill(); };
    process.once('SIGINT', interrupt); process.once('SIGTERM', interrupt);
    const timer = setTimeout(() => { timedOut = true; kill(); }, Math.max(1, timeoutMs));
    for (const [stream, fd] of [['stdout', stdoutFd], ['stderr', stderrFd]]) child[stream].setEncoding('utf8').on('data', data => {
      fs.writeSync(fd, data); bytes += Buffer.byteLength(data);
      if (bytes <= 128 * 1024 * 1024) output[stream] += data;
      else { error = 'Process output exceeds controller capture limit; full output retained in logs'; kill(); }
    });
    child.on('error', e => { error = e.message; });
    child.on('close', (exitCode, signal) => {
      clearTimeout(timer); process.removeListener('SIGINT', interrupt); process.removeListener('SIGTERM', interrupt);
      fs.closeSync(stdoutFd); fs.closeSync(stderrFd);
      resolve({ command, args, cwd, log, exitCode, signal, timedOut, interrupted, error, durationMs: Date.now() - started, ...output });
    });
  });
}
function checked(result) {
  if (result.exitCode !== 0 || result.signal || result.timedOut || result.interrupted || result.error) fail(`Process failed (exit=${result.exitCode}, timeout=${result.timedOut}): ${result.log}`);
  return result.stdout;
}
function reportProcess(result) {
  const { stdout, stderr, ...report } = result;
  return report;
}
function metadataTargets(metadata, profile, units) {
  const packages = profile.packages === 'default-members'
    ? metadata.packages.filter(p => metadata.workspace_default_members.includes(p.id))
    : metadata.packages.filter(p => profile.packages.includes(p.name));
  if (!packages.length || (Array.isArray(profile.packages) && packages.length !== profile.packages.length)) fail('Missing selected Cargo packages');
  const expected = [], excluded = [];
  for (const pkg of packages) {
    const enabled = new Set(profile.allFeatures ? Object.keys(pkg.features) : units.filter(u => u.package_id === pkg.id).flatMap(u => u.features));
    for (const target of pkg.targets) {
      if (!target.test || !target.kind.some(k => profile.kinds.includes(k))) continue;
      const required = target['required-features'] || target.required_features || [];
      const item = { packageId: pkg.id, package: pkg.name, target: target.name, kind: target.kind, manifest: pkg.manifest_path };
      if (required.some(feature => !enabled.has(feature))) excluded.push({ ...item, reason: 'required-features disabled', required });
      else expected.push(item);
    }
  }
  return { packages, expected, excluded };
}
function cargoArtifacts(output) {
  const messages = output.split(/\r?\n/).filter(Boolean).map(line => JSON.parse(line));
  const finished = messages.filter(m => m.reason === 'build-finished');
  if (finished.length !== 1 || finished[0].success !== true) fail('Cargo JSON has no unique successful build-finished receipt');
  return { units: messages.filter(m => m.reason === 'compiler-artifact'), scripts: messages.filter(m => m.reason === 'build-script-executed') };
}
function dynamicFiles(directories) {
  return [...new Set(directories)].flatMap(directory => !fs.existsSync(directory) ? [] : fs.readdirSync(directory)
    .filter(name => /\.(dll|dylib|so(?:\.\d+)*)$/.test(name))
    .map(name => path.join(directory, name))).filter(filename => fs.lstatSync(filename).isFile());
}
function packageEnvironment(pkg) {
  const version = pkg.version.match(/^(\d+)\.(\d+)\.(\d+)(?:-([^+]+))?/);
  return { CARGO_PKG_NAME: pkg.name, CARGO_PKG_VERSION: pkg.version,
    CARGO_PKG_VERSION_MAJOR: version?.[1] || '', CARGO_PKG_VERSION_MINOR: version?.[2] || '',
    CARGO_PKG_VERSION_PATCH: version?.[3] || '', CARGO_PKG_VERSION_PRE: version?.[4] || '',
    CARGO_PKG_AUTHORS: (pkg.authors || []).join(':'),
    ...Object.fromEntries(['description', 'homepage', 'repository', 'license', 'license_file', 'rust_version', 'readme']
      .map(key => ['CARGO_PKG_' + key.toUpperCase(), pkg[key] || ''])),
    CARGO_MANIFEST_DIR: path.dirname(pkg.manifest_path), CARGO_MANIFEST_PATH: pkg.manifest_path };
}
function artifactRecords(metadata, profile, output, root) {
  const { units, scripts } = cargoArtifacts(output);
  const { packages, expected, excluded } = metadataTargets(metadata, profile, units);
  const tests = units.filter(u => u.profile?.test && u.executable);
  const targets = expected.map(wanted => {
    const found = tests.filter(u => u.package_id === wanted.packageId && u.target.name === wanted.target && canonical(u.target.kind) === canonical(wanted.kind));
    if (found.length !== 1) fail(`Expected one Cargo test artifact for ${wanted.package}/${wanted.target}, found ${found.length}`);
    const unit = found[0], pkg = packages.find(p => p.id === unit.package_id);
    const manifest = path.resolve(unit.manifest_path || pkg.manifest_path);
    if (manifest !== path.resolve(pkg.manifest_path) || !manifest.startsWith(root + path.sep)) fail('Artifact manifest differs from selected workspace package');
    const buildScripts = scripts.filter(s => s.package_id === pkg.id);
    if (buildScripts.length > 1) fail(`Ambiguous build-script runtime context: ${pkg.name}`);
    return { ...wanted, id: digest(wanted), executable: path.resolve(unit.executable), cwd: path.dirname(manifest),
      features: unit.features, profile: unit.profile, tests: [],
      packageEnv: packageEnvironment(pkg), buildEnv: Object.fromEntries(buildScripts[0]?.env || []),
      outDir: buildScripts[0]?.out_dir || null };
  });
  if (!targets.length || tests.length !== targets.length) fail('Unexpected, missing or duplicate Cargo test artifacts');
  const files = [...new Set(units.flatMap(u => [u.executable, ...(u.filenames || []).filter(f => /\.(dll|dylib|so(?:\.\d+)*)$/.test(f))]).filter(Boolean))];
  const linkedPaths = scripts.flatMap(s => s.linked_paths || []).map(p => p.replace(/^(native|dependency|crate|framework|all)=/, ''));
  const directories = [...new Set([...files.map(f => path.dirname(f)), ...linkedPaths])];
  return { targets, excluded, files, directories };
}
function runtimeEnv(manifest, target) {
  const dirs = manifest.dynamicDirectories;
  const libraryKey = process.platform === 'win32' ? 'PATH' : process.platform === 'darwin' ? 'DYLD_FALLBACK_LIBRARY_PATH' : 'LD_LIBRARY_PATH';
  const inherited = process.env[libraryKey] || (process.platform === 'darwin'
    ? [path.join(os.homedir(), 'lib'), '/usr/local/lib', '/usr/lib'].join(path.delimiter) : '');
  return { ...process.env, ...manifest.runtimeEnv, ...target.buildEnv, ...target.packageEnv, ...(target.outDir ? { OUT_DIR: target.outDir } : {}),
    [libraryKey]: [...dirs, inherited].filter(Boolean).join(path.delimiter) };
}
function assertManifest(manifest, root, config = CONFIG) {
  const { inputKey, ...body } = manifest;
  if (manifest.schemaVersion !== 1 || digest(body) !== inputKey) fail('Invalid artifact manifest/cache identity');
  if (canonical(identity(root, config)) !== canonical(manifest.identity)) fail('Source, host, environment or runner inputs changed; compile a new profile receipt');
  for (const artifact of manifest.artifacts) verifyArtifact(artifact);
}
async function compile(root, evidence, profileName, { config = CONFIG, execute = processRun } = {}) {
  validateConfig(config);
  const profile = config.profiles[profileName];
  if (!profile) fail(`Unknown build profile: ${profileName}`);
  if (profile.platform && process.platform !== profile.platform) fail(`Profile requires ${profile.platform}`);
  const source = identity(root, config);
  fs.mkdirSync(path.dirname(evidence), { recursive: true });
  fs.mkdirSync(evidence); // Never overwrite another compilation or failed evidence.
  const steps = [];
  const invoke = async (name, command, args) => {
    const result = await execute(command, args, { cwd: root, env: { ...process.env, ...config.runtimeEnv }, timeoutMs: 15 * 60 * 1000, log: path.join(evidence, name) });
    steps.push(reportProcess(result)); writeJSON(path.join(evidence, name + '.json'), reportProcess(result));
    return checked(result);
  };
  try {
    const rustc = await invoke('rustc-version', 'rustc', ['--version', '--verbose']);
    const cargo = await invoke('cargo-version', 'cargo', ['--version', '--verbose']);
    const sysroot = (await invoke('rustc-sysroot', 'rustc', ['--print', 'sysroot'])).trim();
    const targetLibdir = (await invoke('rustc-libdir', 'rustc', ['--print', 'target-libdir'])).trim();
    const metadata = JSON.parse(await invoke('metadata', 'cargo', ['metadata', '--locked', '--no-deps', '--format-version=1']));
    const args = ['test', '--locked', ...profile.cargoArgs, '--no-run', '--message-format=json-render-diagnostics'];
    const output = await invoke('compile', 'cargo', args);
    if (profile.rejectCompileDiagnostic && [output, fs.readFileSync(path.join(evidence, 'compile.stderr.log'), 'utf8')].some(s => s.includes(profile.rejectCompileDiagnostic))) fail('Forbidden macOS linker diagnostic');
    const records = artifactRecords(metadata, profile, output, root);
    const toolFiles = ['rustc', 'cargo'].map(name => path.join(sysroot, 'bin', name + (process.platform === 'win32' ? '.exe' : '')));
    const directories = [...new Set([...records.directories, targetLibdir])];
    const files = [...new Set([...records.files, ...toolFiles, ...dynamicFiles(directories)])].sort();
    const manifest = { schemaVersion: 1, identity: source, profileName, buildProfile: profile, cargoArgs: args, toolchain: { rustc, cargo },
      runtimeEnv: config.runtimeEnv, dynamicDirectories: directories, artifacts: files.map(p => ({ path: p, sha256: fileHash(p) })),
      targets: records.targets, excludedTargets: records.excluded, notCovered: config.notCovered };
    for (const [index, target] of manifest.targets.entries()) {
      const runList = async (suffix, flags) => {
        const result = await execute(target.executable, [...flags, '--list', '--color=never'], { cwd: target.cwd, env: runtimeEnv(manifest, target), timeoutMs: 30000, log: path.join(evidence, `inventory-${index}-${suffix}`) });
        writeJSON(path.join(evidence, `inventory-${index}-${suffix}.json`), reportProcess(result));
        return parseList(checked(result));
      };
      const names = await runList('all', []), ignored = await runList('ignored', ['--ignored']);
      if (ignored.some(name => !names.includes(name))) fail('Ignored --list contains unknown names');
      target.tests = names.map(name => ({ name, ignored: ignored.includes(name) }));
    }
    manifest.inputKey = digest(manifest);
    assertManifest(manifest, root, config); plan(manifest, 'default', config);
    writeJSON(path.join(evidence, 'artifacts.json'), manifest);
    return manifest;
  } catch (e) {
    writeJSON(path.join(evidence, 'compile-failure.json'), { status: 'failed', error: e.message, identity: source, steps }); throw e;
  }
}
function groupKey(manifest, group, mode, commands, config = CONFIG) {
  return digest({ inputKey: manifest.inputKey, group, mode, commands, timeoutSeconds: config.groups[group].timeoutSeconds, testThreads: 1 });
}
function aggregate(manifest, groups, results, mode = 'default', config = CONFIG) {
  const status = {};
  for (const result of results) {
    if (result.inputKey !== manifest.inputKey || !Object.hasOwn(groups, result.group) || !['default', 'only', 'include'].includes(result.mode)) fail('Unknown group/mode or stale result identity');
  }
  for (const [group, commands] of Object.entries(groups).filter(([, commands]) => commands.length)) {
    const key = groupKey(manifest, group, mode, commands, config);
    const attempts = results.filter(r => r.group === group && r.mode === mode);
    if (attempts.some(r => r.inputKey !== manifest.inputKey || r.selectionKey !== key)) fail(`Stale or changed result identity: ${group}`);
    if (new Set(attempts.map(r => r.attempt)).size !== attempts.length) fail(`Duplicate group result: ${group}`);
    const latest = attempts.sort((a, b) => b.attempt - a.attempt)[0];
    status[group] = latest || { group, status: 'not-run', counts: { passed: 0, failed: 0, ignored: 0 }, expected: commands.reduce((n, c) => n + c.names.length, 0) };
    if (latest && (latest.expected !== commands.reduce((n, c) => n + c.names.length, 0) ||
        (latest.status === 'passed' && (latest.exitCode !== 0 || latest.error || latest.counts.failed || latest.counts.passed + latest.counts.ignored !== latest.expected ||
          latest.counts.ignored !== commands.reduce((n, c) => n + c.ignored, 0) ||
          latest.processes.some(p => p.exitCode !== 0 || p.signal || p.timedOut || p.interrupted || p.error || p.countError) ||
          latest.processes.length !== 2 * commands.length)))) fail(`Incomplete passed group receipt: ${group}`);
  }
  const values = Object.values(status);
  const completeWithinSelection = values.length > 0 && values.every(r => r.status === 'passed');
  const inventoried = manifest.targets.reduce((n, t) => n + t.tests.length, 0);
  const selected = Object.values(groups).flat().reduce((n, c) => n + c.names.length, 0);
  const counts = values.reduce((sum, r) => ({ passed: sum.passed + r.counts.passed, failed: sum.failed + r.counts.failed, ignored: sum.ignored + r.counts.ignored }), { passed: 0, failed: 0, ignored: 0 });
  return { inputKey: manifest.inputKey, profileName: manifest.profileName, source: manifest.identity, mode,
    completeWithinSelection, completeWithinProfile: completeWithinSelection && selected === inventoried, fullAcceptance: false,
    coverage: { inventoried, selected, outsideIgnoredMode: inventoried - selected, notAccounted: selected - counts.passed - counts.failed - counts.ignored }, counts,
    groups: status, unavailableGroups: Object.keys(groups).filter(g => !groups[g].length),
    zeroTestTargets: manifest.targets.filter(t => !t.tests.length).map(t => `${t.package}/${t.target}`),
    excludedTargets: manifest.excludedTargets, notCovered: manifest.notCovered };
}
function readResults(evidence) {
  const directory = path.join(evidence, 'runs');
  if (!fs.existsSync(directory)) return [];
  return fs.readdirSync(directory).flatMap(attempt => fs.readdirSync(path.join(directory, attempt), { withFileTypes: true })
    .filter(entry => entry.isDirectory()).flatMap(entry => {
      const file = path.join(directory, attempt, entry.name, 'result.json');
      return fs.existsSync(file) ? [{ ...readJSON(file), evidence: file }] : [];
    }));
}
async function verifySelection(manifest, target, command, log, timeoutMs, execute = processRun) {
  const result = await execute(target.executable, [...command.args, '--list', '--color=never'], { cwd: target.cwd, env: runtimeEnv(manifest, target), timeoutMs, log });
  try { equalNames(parseList(checked(result)), command.names); }
  catch (e) { e.processReport = reportProcess(result); throw e; }
  return reportProcess(result);
}
async function runGroups(root, evidence, requested, { all = false, retryFailed = false, listOnly = false, mode = 'default', config = CONFIG, execute = processRun } = {}) {
  const lock = fs.openSync(path.join(evidence, '.controller-lock'), 'wx');
  try {
    const manifest = readJSON(path.join(evidence, 'artifacts.json'));
    assertManifest(manifest, root, config);
    const groups = plan(manifest, mode, config), previous = readResults(evidence);
    const baseline = aggregate(manifest, groups, previous, mode, config);
    if (retryFailed) {
      if (all || requested.length) fail('retry-failed uses only the failed groups from this exact receipt');
      requested = Object.values(baseline.groups).filter(r => r.status === 'failed').map(r => r.group);
    }
    const selected = selectGroups(groups, requested, all);
    const runs = path.join(evidence, 'runs'); fs.mkdirSync(runs, { recursive: true });
    const attempt = 1 + Math.max(0, ...fs.readdirSync(runs).map(Number));
    const directory = path.join(runs, String(attempt).padStart(6, '0')); fs.mkdirSync(directory);
    const results = [], listResults = [];
    for (const group of selected) {
      const groupDir = path.join(directory, group); fs.mkdirSync(groupDir);
      const commands = groups[group], started = Date.now(), deadline = started + config.groups[group].timeoutSeconds * 1000;
      const result = { inputKey: manifest.inputKey, selectionKey: groupKey(manifest, group, mode, commands, config), group, mode, attempt,
        status: 'failed', expected: commands.reduce((n, c) => n + c.names.length, 0), counts: { passed: 0, failed: 0, ignored: 0 },
        timeoutSeconds: config.groups[group].timeoutSeconds, startedAt: new Date(started).toISOString(), processes: [] };
      try {
        // Preflight the whole group before any tests. Every planned name appears once.
        for (const [index, command] of commands.entries()) {
          if (Date.now() >= deadline) fail('Group timed out during --list');
          const target = manifest.targets.find(t => t.id === command.targetId);
          result.processes.push(await verifySelection(manifest, target, command, path.join(groupDir, `list-${index}`), deadline - Date.now(), execute));
        }
        if (!listOnly) for (const [index, command] of commands.entries()) {
          assertManifest(manifest, root, config);
          if (Date.now() >= deadline) fail('Group timed out');
          const target = manifest.targets.find(t => t.id === command.targetId);
          const processResult = await execute(target.executable, [...command.args, '--test-threads=1', '--format=pretty', '--color=never'],
            { cwd: target.cwd, env: runtimeEnv(manifest, target), timeoutMs: deadline - Date.now(), log: path.join(groupDir, `run-${index}`) });
          const processReport = reportProcess(processResult); result.processes.push(processReport);
          let counts;
          try {
            counts = parseCounts(processResult.stdout, command.names.length, command.ignored);
            processReport.counts = counts;
            for (const key of ['passed', 'failed', 'ignored']) result.counts[key] += counts[key];
          } catch (e) { processReport.countError = e.message; }
          if (!counts || counts.failed || processResult.exitCode !== 0 || processResult.signal || processResult.timedOut || processResult.interrupted || processResult.error) result.error ||= `Failed runtime/count verification: ${processResult.log}`;
          if (processResult.interrupted) break;
        }
        assertManifest(manifest, root, config);
        if (!result.error) result.status = listOnly ? 'listed' : 'passed';
      } catch (e) { result.error = e.message; if (e.processReport) result.processes.push(e.processReport); }
      result.durationMs = Date.now() - started;
      result.exitCode = result.status === 'passed' || result.status === 'listed' ? 0 : 1;
      writeJSON(path.join(groupDir, listOnly ? 'list-result.json' : 'result.json'), result);
      if (!listOnly) results.push(result); else listResults.push(result);
      if (result.processes.some(p => p.interrupted)) break;
    }
    const summary = aggregate(manifest, groups, [...previous, ...results], mode, config);
    writeJSON(path.join(directory, 'summary.json'), summary);
    return { summary, selected, results, listResults, directory, listOnly };
  } finally {
    fs.closeSync(lock); fs.unlinkSync(path.join(evidence, '.controller-lock'));
  }
}
function argumentsFor(argv) {
  const command = argv.shift(), options = { group: [] };
  for (let index = 0; index < argv.length; index++) {
    const arg = argv[index];
    if (arg === '--all') { if (options.all) fail('Duplicate --all'); options.all = true; }
    else if (['--profile', '--evidence', '--group', '--ignored'].includes(arg)) {
      const value = argv[++index]; if (!value || value.startsWith('--')) fail(`Missing value for ${arg}`);
      if (arg === '--group') options.group.push(value);
      else { const key = arg.slice(2); if (options[key]) fail(`Duplicate ${arg}`); options[key] = value; }
    } else fail(`Unknown option: ${arg}`);
  }
  if (!['groups', 'compile', '--list', 'run', 'retry-failed', 'summary'].includes(command)) fail('Commands: groups, compile --profile NAME --evidence DIR, --list/run --evidence DIR --group NAME|--all, retry-failed/summary --evidence DIR; optional --ignored default|only|include');
  if (command !== 'compile' && options.profile) fail('Build profile comes from the compiled receipt');
  if (command === 'compile' && (!options.profile || options.all || options.group.length || options.ignored)) fail('compile requires only --profile NAME and --evidence DIR');
  if (['groups', 'summary', 'retry-failed'].includes(command) && (options.all || options.group.length)) fail(`${command} does not accept group selection`);
  return { command, options };
}
async function main(argv) {
  const { command, options } = argumentsFor([...argv]); validateConfig(CONFIG);
  if (command === 'groups') return console.log(JSON.stringify({ profiles: CONFIG.profiles, groups: CONFIG.groups, notCovered: CONFIG.notCovered }, null, 2));
  if (!options.evidence) fail('Specify an owned --evidence DIR');
  const evidence = path.resolve(options.evidence), mode = options.ignored || 'default';
  if (command === 'compile') {
    const manifest = await compile(ROOT, evidence, options.profile);
    console.log(JSON.stringify({ inputKey: manifest.inputKey, profile: manifest.profileName, targets: manifest.targets.length, evidence }, null, 2)); return;
  }
  if (command === 'summary') {
    const manifest = readJSON(path.join(evidence, 'artifacts.json')); assertManifest(manifest, ROOT);
    const summary = aggregate(manifest, plan(manifest, mode), readResults(evidence), mode);
    console.log(JSON.stringify(summary, null, 2)); process.exitCode = summary.completeWithinProfile ? 0 : 1; return;
  }
  const result = await runGroups(ROOT, evidence, options.group, { all: options.all, retryFailed: command === 'retry-failed', listOnly: command === '--list', mode });
  console.log(JSON.stringify({ evidence: result.directory, selected: result.selected,
    ...(result.listOnly ? { selections: result.selected.flatMap(g => plan(readJSON(path.join(evidence, 'artifacts.json')), mode)[g].map(c => ({ group: g, ...c }))) } : {}), summary: result.summary }, null, 2));
  process.exitCode = (result.listOnly ? result.listResults.every(r => r.status === 'listed') : result.results.every(r => r.status === 'passed')) ? 0 : 1;
}
module.exports = { CONFIG, canonical, digest, fileHash, identity, validateConfig, route, parseList, equalNames, parseCounts, partition, plan, selectGroups,
  processRun, metadataTargets, cargoArtifacts, artifactRecords, assertManifest, compile, groupKey, aggregate, runGroups, argumentsFor };
if (require.main === module) main(process.argv.slice(2)).catch(e => { console.error(e.message); process.exitCode = 1; });
