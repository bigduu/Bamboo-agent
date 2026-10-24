#!/usr/bin/env node
'use strict';
// Actions adapter for the shared libtest controller. Build once; runtime never calls Cargo.
const fs = require('node:fs');
const path = require('node:path');
const {spawnSync} = require('node:child_process');
const r = require('./test-groups.cjs');
const ROOT = path.resolve(__dirname, '..');
const fail = message => { throw new Error(message); };
const MAX_SECONDS = 600;
const MATRIX_LIMIT = 256;
const RUNTIME_FAMILIES = 2;
const PACKAGE = /^[a-z0-9][a-z0-9-]*$/;
const read = r.readJSON;
const write = r.writeJSON;
const exists = filename => fs.existsSync(filename);
function output(key, value) {
  if (process.env.GITHUB_OUTPUT) fs.appendFileSync(process.env.GITHUB_OUTPUT, `${key}=${value}\n`);
}
function shell(command, args, options = {}) {
  const result = spawnSync(command, args, {encoding: 'utf8', maxBuffer: 32 * 1024 * 1024, timeout: 120000, ...options});
  if (result.error || result.status !== 0) fail(`${command} failed: ${result.error?.message || result.stderr}`);
  return result.stdout;
}
function owned(filename, root = ROOT) {
  if (!path.resolve(filename).startsWith(path.join(root, 'target') + path.sep)) fail(`Not an owned Cargo output: ${filename}`);
  return filename;
}
function filesUnder(directory) {
  if (!exists(directory)) return [];
  return fs.readdirSync(directory, {withFileTypes: true}).flatMap(entry => {
    const filename = path.join(directory, entry.name);
    if (entry.isDirectory()) return filesUnder(filename);
    if (entry.isSymbolicLink() && !fs.realpathSync(filename).startsWith(ROOT + path.sep)) fail('Build output link leaves the checkout');
    return fs.statSync(filename).isFile() ? [filename] : [];
  });
}
function blockId(pkg, target, topic, index, names) {
  return `${pkg}-${target}-${topic}-${index}`.replace(/[^a-zA-Z0-9-]/g, '-').slice(0, 105) + '-' + r.digest(names).slice(0, 10);
}
function inventoryKeys(manifest) { return manifest.targets.flatMap(t => t.tests.map(test => `${t.id}\0${test.name}`)).sort(); }
function makeBlocks(manifest, docs = []) {
  const blocks = [];
  for (const target of manifest.targets) {
    const topics = {};
    for (const test of target.tests) (topics[r.route(target, test.name)] ||= []).push(test.name);
    for (const [topic, names] of Object.entries(topics)) {
      names.sort();
      const size = target.kind.includes('test') ? 16 : 128;
      for (let start = 0; start < names.length; start += size) {
        const chosen = names.slice(start, start + size), index = start / size;
        const commands = r.partition(target, chosen).map(command => ({...command, targetId: target.id,
          ignored: target.tests.filter(test => test.ignored && command.names.includes(test.name)).length}));
        blocks.push({id: blockId(target.package, target.target, topic, index, chosen), package: target.package,
          target: target.target, topic, kind: manifest.profileName === 'ssh-live' ? 'ssh' : 'libtest', targetId: target.id,
          names: chosen, commands, timeoutSeconds: MAX_SECONDS});
      }
    }
  }
  const selected = blocks.flatMap(b => b.names.map(name => `${b.targetId}\0${name}`)).sort();
  r.equalNames(selected, inventoryKeys(manifest));
  if (new Set(selected).size !== selected.length) fail('Duplicate CI inventory selection');
  const packages = [...new Set(manifest.targets.map(t => t.package))];
  for (const pkg of packages) if (!blocks.some(b => b.package === pkg)) fail(`Crate has zero tests: ${pkg}; record an explicit source/coverage repair instead of a green skip`);
  for (const doc of docs.filter(d => d.names.length)) blocks.push({id: blockId(doc.package, doc.id, 'doctest', 0, doc.names),
    package: doc.package, target: doc.id, topic: 'doctest', kind: 'doctest', docId: doc.id, names: doc.names, timeoutSeconds: MAX_SECONDS});
  const runtime = blocks.filter(b => b.kind !== 'doctest'), doctests = blocks.filter(b => b.kind === 'doctest');
  if (!blocks.length || runtime.length > MATRIX_LIMIT * RUNTIME_FAMILIES || doctests.length > MATRIX_LIMIT) fail('Empty inventory or Actions matrix family capacity exceeded; no tests were omitted');
  const split = Math.ceil(runtime.length / RUNTIME_FAMILIES);
  runtime.forEach((block, index) => { block.matrixFamily = index < split ? 1 : 2; });
  doctests.forEach(block => { block.matrixFamily = 1; });
  if (new Set(blocks.map(b => b.id)).size !== blocks.length) fail('Duplicate block identity');
  return blocks;
}
function matrixBlocks(blocks, family, doctest = false) {
  if (![1, 2].includes(family) || (doctest && family !== 1)) fail('Unknown matrix family');
  const selected = blocks.filter(b => (b.kind === 'doctest') === doctest && b.matrixFamily === family);
  if (selected.length > MATRIX_LIMIT) fail('Actions matrix exceeds 256');
  return {include: selected.map(b => ({id: b.id}))};
}
function emptyMatrixJobs(plan) {
  if (plan.manifest.profileName === 'ssh-live' || matrixBlocks(plan.blocks, 2).include.length) return [];
  return [(plan.manifest.profileName === 'default' ? 'Test' : 'E2E') + ' Block 2 (${{ matrix.id }})'];
}
function validatePlan(plan) {
  const {planKey, ...body} = plan;
  if (plan.schema !== 'bamboo-ci-test-plan/v1' || r.digest(body) !== planKey) fail('CI plan digest mismatch');
  if (r.digest({...plan.manifest, inputKey: undefined}) !== plan.manifest.inputKey) fail('Compiler manifest digest mismatch');
  const expected = makeBlocks(plan.manifest, plan.docs);
  if (r.canonical(expected) !== r.canonical(plan.blocks)) fail('Missing, duplicated or modified CI inventory block');
  if (plan.blocks.some(block => !PACKAGE.test(block.package) || block.timeoutSeconds !== MAX_SECONDS)) fail('Every block must retain one crate and a 600-second maximum');
}
function assertSource(plan) {
  validatePlan(plan);
  if (r.canonical(r.identity(ROOT, r.CONFIG, 'ci')) !== r.canonical(plan.manifest.identity)) fail('CI source/build environment differs from the compiled plan');
}
async function artifactClient() { return (await import('./ci-artifacts/client.mjs')).client; }
function descriptor(filename) {
  return {path: filename, sha256: r.fileHash(fs.realpathSync(filename)), mode: fs.statSync(filename).mode & 0o777};
}
function pack(directory, name, filenames) {
  const stage = path.join(directory, name); fs.mkdirSync(stage); fs.mkdirSync(path.join(stage, 'files'));
  const members = [...new Set(filenames)].sort().map((filename, index) => {
    owned(filename); const item = {...descriptor(filename), file: `files/${index}`};
    fs.copyFileSync(filename, path.join(stage, item.file)); fs.chmodSync(path.join(stage, item.file), item.mode);
    return item;
  });
  write(path.join(stage, 'members.json'), members);
  const archive = path.join(directory, name + '.tar.gz');
  shell('tar', ['-czf', archive, '-C', stage, 'members.json', 'files']);
  fs.rmSync(stage, {recursive: true});
  return {archive, sha256: r.fileHash(archive), members};
}
function unpack(archive, expected, directory) {
  if (r.fileHash(archive) !== expected.sha256) fail('Artifact archive bytes changed');
  fs.mkdirSync(directory);
  const listing = shell('tar', ['-tzf', archive]).trim().split(/\r?\n/);
  if (new Set(listing).size !== listing.length || listing.some(name => !/^(members\.json|files\/?|files\/\d+)$/.test(name))) fail('Unsafe archive members');
  const types = shell('tar', ['-tvzf', archive]).trim().split(/\r?\n/);
  if (types.some(line => !/^[-d]/.test(line))) fail('Links/devices are forbidden in CI transport');
  shell('tar', ['-xzf', archive, '-C', directory]);
  const members = read(path.join(directory, 'members.json'));
  if (r.canonical(members) !== r.canonical(expected.members)) fail('Artifact member manifest changed');
  for (const item of members) {
    owned(item.path);
    const source = path.join(directory, item.file);
    if (r.fileHash(source) !== item.sha256) fail(`Transported bytes changed: ${item.path}`);
    fs.mkdirSync(path.dirname(item.path), {recursive: true}); fs.copyFileSync(source, item.path); fs.chmodSync(item.path, item.mode);
  }
}
async function captureDocs(manifest, evidence) {
  const directory = path.join(evidence, 'doc-inventory'); fs.mkdirSync(directory);
  const rustdoc = path.join(manifest.toolchain.sysroot, 'bin', 'rustdoc');
  const result = await r.processRun('cargo', ['test', '--locked', '--doc', '--no-fail-fast', '--message-format=json-render-diagnostics'],
    {cwd: ROOT, env: {...process.env, ...r.CONFIG.runtimeEnv, RUSTDOC: path.join(ROOT, 'scripts/test-doc-inventory.cjs'),
      BAMBOO_REAL_RUSTDOC: rustdoc, BAMBOO_DOC_INVENTORY: directory}, timeoutMs: 15 * 60 * 1000, log: path.join(evidence, 'doc-dependencies')});
  write(path.join(evidence, 'doc-dependencies.json'), r.reportProcess(result)); r.checked(result);
  const metadata = read(path.join(evidence, 'metadata.stdout.log'));
  const selected = metadata.packages.filter(p => metadata.workspace_default_members.includes(p.id));
  const expected = selected.filter(p => p.targets.some(t => t.kind.includes('lib') && t.doctest)).map(p => p.name).sort();
  const docs = fs.readdirSync(directory).filter(name => name.endsWith('.invocation.json')).map(name => {
    const invocation = read(path.join(directory, name)), listing = read(path.join(directory, name.replace('.invocation.json', '.result.json')));
    return {...invocation, names: listing.names, ignoredNames: listing.ignoredNames, executableSha256: r.fileHash(invocation.executable)};
  });
  r.equalNames(docs.map(d => d.package), expected);
  const {units, scripts} = r.cargoArtifacts(result.stdout);
  return {docs, files: units.flatMap(u => u.filenames || []), outputs: scripts.map(s => s.out_dir).filter(Boolean), durationMs: result.durationMs};
}
async function build(profile, directory, client) {
  const started = Date.now();
  const manifest = await r.compile(ROOT, directory, profile, {identityMode: 'ci'});
  const docInventory = profile === 'default' ? await captureDocs(manifest, directory) : {docs: [], files: [], outputs: [], durationMs: 0};
  const blocks = makeBlocks(manifest, docInventory.docs), binaries = new Set(manifest.targets.map(t => t.executable));
  const prefix = `bamboo-tests-${process.env.GITHUB_SHA}-${profile}-${process.env.GITHUB_RUN_ID}-${process.env.GITHUB_RUN_ATTEMPT}`;
  const transport = path.join(directory, 'transport'); fs.mkdirSync(transport);
  const supportFiles = [...manifest.artifacts.map(a => a.path).filter(p => !binaries.has(p) && p.startsWith(ROOT + path.sep)),
    ...manifest.buildOutputs.flatMap(filesUnder)];
  const upload = async (suffix, files) => {
    const packed = pack(transport, suffix, files), name = `${prefix}-${suffix}`;
    const result = await client.uploadArtifact(name, [packed.archive], transport, {retentionDays: 1, compressionLevel: 0});
    if (!Number.isSafeInteger(result.id) || result.id <= 0) fail('Artifact upload has no immutable ID');
    return {...packed, archive: path.basename(packed.archive), id: result.id, name};
  };
  const support = await upload('support', supportFiles), targetBundles = {};
  for (const target of manifest.targets.filter(t => t.tests.length)) targetBundles[target.id] = await upload(target.id.slice(0, 16), [target.executable]);
  const docBundle = docInventory.docs.length ? await upload('doctest-graph', [...manifest.dependencyFiles, ...docInventory.files,
    ...manifest.buildOutputs.flatMap(filesUnder), ...docInventory.outputs.flatMap(filesUnder)].filter(p => p.startsWith(ROOT + path.sep))) : null;
  r.assertManifest(manifest, ROOT);
  const plan = {schema: 'bamboo-ci-test-plan/v1', manifest, docs: docInventory.docs, blocks,
    support, targetBundles, docBundle, buildDurationMs: Date.now() - started, docInventoryDurationMs: docInventory.durationMs,
    producerAttempt: Number(process.env.GITHUB_RUN_ATTEMPT), prefix};
  plan.planKey = r.digest(plan); validatePlan(plan);
  const planPath = path.join(directory, 'plan.json'); write(planPath, plan);
  const saved = await client.uploadArtifact(prefix + '-plan', [planPath], directory, {retentionDays: 7, compressionLevel: 6});
  if (!Number.isSafeInteger(saved.id) || saved.id <= 0) fail('Plan artifact upload failed');
  output('plan_id', saved.id); output('plan_key', plan.planKey); output('node', process.version);
  output('blocks', JSON.stringify(matrixBlocks(blocks, 1)));
  output('blocks_2', JSON.stringify(matrixBlocks(blocks, 2)));
  output('has_blocks_2', String(matrixBlocks(blocks, 2).include.length > 0));
  output('docs', JSON.stringify(matrixBlocks(blocks, 1, true)));
  output('has_docs', String(blocks.some(b => b.kind === 'doctest')));
  const publication = {schema: plan.schema, sha: manifest.identity.sha, tree: manifest.identity.tree, profile, planKey: plan.planKey,
    planId: saved.id, producerAttempt: plan.producerAttempt, inventory: inventoryKeys(manifest).length,
    doctests: plan.docs.reduce((n, d) => n + d.names.length, 0), packages: [...new Set(manifest.targets.map(t => t.package))].sort(),
    blocks: blocks.map(b => ({id: b.id, package: b.package, kind: b.kind, matrixFamily: b.matrixFamily, count: b.names.length, timeoutSeconds: b.timeoutSeconds,
      ignored: b.kind === 'doctest' ? plan.docs.find(d => d.id === b.docId).ignoredNames.length : b.kind === 'ssh' ? 0 : b.commands.reduce((n, c) => n + c.ignored, 0)}))};
  console.log('GROUPED_TEST_PLAN ' + Buffer.from(JSON.stringify(publication)).toString('base64'));
  console.log(`Build ${profile}: ${plan.buildDurationMs} ms; runtime tests executed: 0`);
  return plan;
}
async function downloadPlan(id, key, directory, client) {
  const downloaded = path.join(directory, 'plan-download'); fs.mkdirSync(downloaded, {recursive: true});
  await client.downloadArtifact(Number(id), {path: downloaded});
  const plan = read(path.join(downloaded, 'plan.json'));
  if (plan.planKey !== key) fail('Wrong compile-plan artifact'); assertSource(plan); return plan;
}
async function restoreBundle(record, directory, label, client) {
  const download = path.join(directory, label + '-download'); fs.mkdirSync(download);
  await client.downloadArtifact(record.id, {path: download});
  unpack(path.join(download, record.archive), record, path.join(directory, label + '-unpack'));
}
function verifyRuntimeFiles(plan, block) {
  const testFiles = new Set(plan.manifest.targets.map(t => t.executable));
  const selected = plan.manifest.targets.find(t => t.id === block.targetId)?.executable;
  for (const file of plan.manifest.artifacts.filter(a => !testFiles.has(a.path) || a.path === selected))
    if (r.fileHash(file.path) !== file.sha256) fail(`Runtime input bytes changed: ${file.path}`);
  if (block.kind === 'doctest') {
    const doc = plan.docs.find(d => d.id === block.docId);
    if (r.fileHash(doc.executable) !== doc.executableSha256) fail('Doctest compiler changed');
  }
}
function docCounts(output, expected) {
  const summaries = output.split(/\r?\n/).filter(line => /^test result:/.test(line));
  if (!summaries.length) fail('No doctest runtime summary');
  const counts = {passed: 0, failed: 0, ignored: 0};
  for (const line of summaries) {
    const match = line.match(/^test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored; 0 measured; \d+ filtered out; finished in .+$/);
    if (!match || (match[1] === 'ok') !== (Number(match[3]) === 0)) fail('Invalid doctest runtime summary');
    ['passed', 'failed', 'ignored'].forEach((key, index) => counts[key] += Number(match[index + 2]));
  }
  if (counts.passed + counts.failed + counts.ignored !== expected) fail('Doctest runtime counts do not cover the inventory');
  return counts;
}
async function executeBlock(plan, block, directory, deadline, execute = r.processRun) {
  const started = Date.now(), result = {schema: 'bamboo-ci-test-result/v1', planKey: plan.planKey, block: block.id, package: block.package,
    profile: plan.manifest.profileName, sha: plan.manifest.identity.sha, sourceTree: plan.manifest.identity.tree,
    attempt: Number(process.env.GITHUB_RUN_ATTEMPT), status: 'failed', exitCode: 1, expected: block.names.length,
    timeoutSeconds: MAX_SECONDS, preparationDurationMs: Math.max(0, started - Number(process.env.BAMBOO_BLOCK_STARTED_AT || started / 1000) * 1000),
    counts: {passed: 0, failed: 0, ignored: 0}, processes: []};
  const invoke = async (label, executable, args, cwd, env) => {
    if (Date.now() >= deadline) fail('Block exceeded its cumulative 10-minute budget');
    const p = await execute(executable, args, {cwd, env, timeoutMs: deadline - Date.now(), log: path.join(directory, label)});
    result.processes.push(r.reportProcess(p)); return p;
  };
  try {
    assertSource(plan); verifyRuntimeFiles(plan, block);
    if (block.kind === 'doctest') {
      const doc = plan.docs.find(d => d.id === block.docId), env = {...process.env, ...doc.env, ...r.CONFIG.runtimeEnv};
      const listing = await invoke('list', doc.executable, [...doc.args, '--test-args=--list'], doc.cwd, env);
      r.equalNames(r.parseList(r.checked(listing)), block.names);
      const p = await invoke('run', doc.executable, doc.args, doc.cwd, env);
      result.counts = docCounts(p.stdout, block.names.length); r.checked(p);
      if (result.counts.ignored !== doc.ignoredNames.length) fail('Doctest ignored semantics changed');
      if (result.counts.failed) fail('Doctest failure');
    } else {
      const target = plan.manifest.targets.find(t => t.id === block.targetId), env = r.runtimeEnv(plan.manifest, target);
      for (const [index, command] of block.commands.entries()) {
        const args = [...command.args, ...(block.kind === 'ssh' ? ['--ignored'] : [])];
        const listed = await invoke(`list-${index}`, target.executable, [...args, '--list', '--color=never'], target.cwd, env);
        r.equalNames(r.parseList(r.checked(listed)), command.names);
      }
      for (const [index, command] of block.commands.entries()) {
        const p = block.kind === 'ssh'
          ? await invoke(`run-${index}`, 'bash', [path.join(ROOT, 'scripts/run-russh-live.sh')], ROOT, {...env, BAMBOO_RUSSH_TEST_EXECUTABLE: target.executable})
          : await invoke(`run-${index}`, target.executable, [...command.args, '--format=pretty', '--color=never'], target.cwd, env);
        const counts = r.parseCounts(p.stdout, command.names.length, block.kind === 'ssh' ? 0 : command.ignored);
        for (const key of ['passed', 'failed', 'ignored']) result.counts[key] += counts[key];
        if (p.exitCode !== 0 || p.signal || p.error || p.timedOut || p.interrupted || counts.failed) result.error ||= `Failed process/counts: ${p.log}`;
      }
    }
    assertSource(plan); verifyRuntimeFiles(plan, block);
    if (!result.error) { result.status = 'passed'; result.exitCode = 0; }
  } catch (e) { result.error = e.message; }
  result.testDurationMs = Date.now() - started;
  if (result.testDurationMs > MAX_SECONDS * 1000) { result.status = 'failed'; result.exitCode = 1; result.error = 'Block exceeded 600 seconds'; }
  write(path.join(directory, 'result.json'), result); return result;
}
async function run(id, key, blockId, directory, client) {
  fs.mkdirSync(directory, {recursive: true});
  const plan = await downloadPlan(id, key, directory, client), block = plan.blocks.find(b => b.id === blockId);
  if (!block || !block.names.length) fail('Unknown or empty CI block');
  await restoreBundle(plan.support, directory, 'support', client);
  await restoreBundle(block.kind === 'doctest' ? plan.docBundle : plan.targetBundles[block.targetId], directory, 'target', client);
  const logs = path.join(directory, 'logs'); fs.mkdirSync(logs);
  const epoch = Number(process.env.BAMBOO_BLOCK_STARTED_AT) * 1000;
  if (!Number.isFinite(epoch) || epoch <= 0) fail('Block job start timestamp is required');
  // Include preparation in the 10-minute job budget; reserve 45s to retain evidence.
  const result = await executeBlock(plan, block, logs, Math.min(epoch + 555000, Date.now() + 555000));
  const name = `${plan.prefix}-result-${block.id}-attempt-${process.env.GITHUB_RUN_ATTEMPT}`;
  const saved = await client.uploadArtifact(name, filesUnder(logs), logs, {retentionDays: 7, compressionLevel: 6});
  if (!Number.isSafeInteger(saved.id) || saved.id <= 0) fail('Result upload has no immutable artifact ID');
  const {processes, ...publicResult} = result;
  console.log('GROUPED_TEST_RESULT ' + Buffer.from(JSON.stringify(publicResult)).toString('base64')); process.exitCode = result.exitCode;
  return result;
}
function jobName(plan, block) {
  if (plan.manifest.profileName === 'ssh-live') return 'Test SSH';
  return `${plan.manifest.profileName === 'default' ? 'Test' : 'E2E'} ${block.kind === 'doctest' ? 'Doctest' : 'Block' + (block.matrixFamily === 2 ? ' 2' : '')} (${block.id})`;
}
function selectJobResults(plan, jobs, results) {
  validatePlan(plan);
  const counts = {passed: 0, failed: 0, ignored: 0};
  const expectedNames = new Set(plan.blocks.map(block => jobName(plan, block)));
  for (const name of emptyMatrixJobs(plan)) {
    const found = jobs.filter(job => job.name === name);
    if (found.length !== 1 || found[0].status !== 'completed' || found[0].conclusion !== 'skipped' ||
        found[0].run_id !== Number(process.env.GITHUB_RUN_ID) || !Number.isInteger(found[0].run_attempt) ||
        found[0].run_attempt < plan.producerAttempt || found[0].run_attempt > Number(process.env.GITHUB_RUN_ATTEMPT)) fail('Unused matrix family must be explicitly empty and skipped');
    expectedNames.add(name);
  }
  const prefix = plan.manifest.profileName === 'default' ? /^Test (Block(?: [0-9]+)?|Doctest) \(/ : plan.manifest.profileName === 'all-features' ? /^E2E Block(?: [0-9]+)? \(/ : /^Test SSH$/;
  if (jobs.some(job => prefix.test(job.name) && !expectedNames.has(job.name))) fail('Unexpected CI block outside the frozen inventory');
  for (const block of plan.blocks) {
    const matches = jobs.filter(job => job.name === jobName(plan, block));
    if (matches.length !== 1) fail(`Missing/ambiguous block job: ${block.id}`);
    const job = matches[0];
    if (job.status !== 'completed' || job.conclusion !== 'success' || job.run_id !== Number(process.env.GITHUB_RUN_ID) ||
        !Number.isInteger(job.run_attempt) || job.run_attempt < plan.producerAttempt || job.run_attempt > Number(process.env.GITHUB_RUN_ATTEMPT)) fail(`Failed, skipped or mismatched block job: ${block.id}`);
    const found = results.filter(value => value.block === block.id && value.attempt === job.run_attempt);
    if (found.length !== 1) fail(`Missing/ambiguous latest-attempt result: ${block.id}`);
    const result = found[0];
    const ignored = block.kind === 'doctest' ? plan.docs.find(d => d.id === block.docId).ignoredNames.length : block.kind === 'ssh' ? 0 : block.commands.reduce((n, c) => n + c.ignored, 0);
    if (result.planKey !== plan.planKey || result.sha !== plan.manifest.identity.sha || result.sourceTree !== plan.manifest.identity.tree || result.package !== block.package ||
        result.profile !== plan.manifest.profileName || result.status !== 'passed' || result.exitCode !== 0 || result.error || result.expected !== block.names.length ||
        result.testDurationMs > MAX_SECONDS * 1000 || result.timeoutSeconds !== MAX_SECONDS || result.counts.failed !== 0 ||
        result.counts.ignored !== ignored || result.counts.passed + result.counts.ignored !== block.names.length ||
        result.processes.length !== (block.kind === 'doctest' ? 2 : 2 * block.commands.length) ||
        result.processes.some(p => p.exitCode !== 0 || p.timedOut || p.interrupted || p.signal || p.error)) fail(`Invalid block result: ${block.id}`);
    for (const k of ['passed', 'failed', 'ignored']) counts[k] += result.counts[k];
  }
  return {planKey: plan.planKey, profile: plan.manifest.profileName, sha: plan.manifest.identity.sha, tree: plan.manifest.identity.tree,
    inventory: inventoryKeys(plan.manifest).length,
    doctests: plan.docs.reduce((n, d) => n + d.names.length, 0), blocks: plan.blocks.length, counts, status: 'passed'};
}
function currentJobs() {
  const jobs = [];
  for (let page = 1; page <= 20; page++) {
    const response = JSON.parse(shell('gh', ['api', `repos/${process.env.GITHUB_REPOSITORY}/actions/runs/${process.env.GITHUB_RUN_ID}/jobs?filter=latest&per_page=100&page=${page}`]));
    jobs.push(...response.jobs); if (response.jobs.length < 100) return jobs;
  }
  fail('Job inventory exceeds the bounded 2000-job limit');
}
async function summary(id, key, directory, client) {
  fs.mkdirSync(directory, {recursive: true}); const plan = await downloadPlan(id, key, directory, client);
  const jobs = currentJobs(), {artifacts} = await client.listArtifacts(), results = [];
  for (const block of plan.blocks) {
    const job = jobs.filter(j => j.name === jobName(plan, block));
    if (job.length !== 1 || job[0].conclusion !== 'success') fail(`Block did not succeed: ${block.id}`);
    const name = `${plan.prefix}-result-${block.id}-attempt-${job[0].run_attempt}`;
    const found = artifacts.filter(a => a.name === name);
    if (found.length !== 1) fail(`Missing/ambiguous result artifact: ${name}`);
    const destination = path.join(directory, block.id); fs.mkdirSync(destination);
    await client.downloadArtifact(found[0].id, {path: destination}); results.push(read(path.join(destination, 'result.json')));
  }
  const result = selectJobResults(plan, jobs, results); write(path.join(directory, 'summary.json'), result);
  console.log('GROUPED_TEST_SUMMARY ' + Buffer.from(JSON.stringify(result)).toString('base64')); return result;
}
async function main(argv) {
  const [command, ...args] = argv;
  if (!['build', 'run', 'summary'].includes(command)) fail('Commands: build PROFILE DIR; run PLAN_ID PLAN_KEY BLOCK_ID DIR; summary PLAN_ID PLAN_KEY DIR');
  if (process.env.GITHUB_ACTIONS !== 'true') fail('CI adapter requires an Actions environment');
  if (command !== 'build' && (!/^[1-9][0-9]*$/.test(args[0] || '') || !/^[0-9a-f]{64}$/.test(args[1] || '')))
    fail('Upstream build did not publish a valid compiled plan; no runtime test result is claimed');
  const client = await artifactClient();
  if (command === 'build') return build(args[0], path.resolve(args[1]), client);
  if (command === 'run') return run(args[0], args[1], args[2], path.resolve(args[3]), client);
  return summary(args[0], args[1], path.resolve(args[2]), client);
}
module.exports = {main, makeBlocks, matrixBlocks, emptyMatrixJobs, validatePlan, blockId, inventoryKeys, docCounts, selectJobResults, jobName, executeBlock, pack, unpack};
if (require.main === module) main(process.argv.slice(2)).catch(e => { console.error(e.message); process.exitCode = 1; });
