'use strict';
// Owned-pipe admission only: synthetic streams/readers, no native process,
// Chromium, image, credentials or real profile.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { PassThrough } = require('node:stream');
const { test } = require('node:test');

const source = fs.readFileSync(process.env.BAMBOO_TEST_HOST_SOURCE || path.join(__dirname, 'host.cjs'), 'utf8');
const attempt = '52a41e83-c85d-4560-a52c-c10c7ca398a0';
const generation = 'b165d584-ad41-4ce3-83f1-ad6a91471270';
const prefix = 'BAMBOO_BROWSER_LAUNCH_V1 ';
function fixture({ legacy = false, missingAttempt = false, preflightFailure = false } = {}) {
  const input = new PassThrough();
  const output = [];
  const events = [];
  let imports = 0;
  const env = legacy ? {} : { BAMBOO_BROWSER_HOST_PROTOCOL: '2',
    BAMBOO_BROWSER_STORAGE_DATA_ROOT: '/synthetic/data',
    ...(missingAttempt ? {} : { BAMBOO_BROWSER_LAUNCH_ATTEMPT: attempt }) };
  const scope = vm.createContext({
    require(name) {
      if (name === 'playwright-core') { imports++; throw new Error('native import forbidden'); }
      return require(name);
    }, module: { exports: {} }, Buffer, URL, setTimeout, clearTimeout, setInterval, clearInterval,
    process: { env, platform: 'darwin', getuid: () => 501, stdin: input,
      stdout: { write: line => output.push(line) } }, events, preflightFailure,
  });
  new vm.Script(source).runInContext(scope);
  vm.runInContext(`productionStorageSettings = (_env, _readers, preflight) => {
    events.push(preflight ? 'preflight' : 'admitted');
    if (preflightFailure) throw Object.assign(new Error('synthetic storage refusal'), { code: 'synthetic_storage_refusal' });
    return { scratch: '/synthetic/data/mounted/tmp/owned' };
  };`, scope);
  return { input, output, events, env, imports: () => imports,
    prepare: () => vm.runInContext('prepareNativeLaunch()', scope) };
}
function grant(changed = {}) {
  return prefix + JSON.stringify({ kind: 'bamboo_browser_launch_grant', version: 1,
    attempt_id: attempt, launch_generation: generation, ...changed }) + '\n';
}
function cleanup(f, result) {
  result?.lines.close(); f.input.destroy();
  assert.equal(f.imports(), 0);
}

test('explicit legacy host has no native admission stage', async () => {
  const f = fixture({ legacy: true });
  try { assert.equal(await f.prepare(), null); assert.deepEqual(f.events, []); assert.deepEqual(f.output, []); }
  finally { cleanup(f); }
});

test('bounded host refuses an old parent without a launch attempt before preflight', async () => {
  const f = fixture({ missingAttempt: true });
  try { await assert.rejects(f.prepare(), error => error.code === 'browser_launch_admission_required');
    assert.deepEqual(f.events, []); assert.deepEqual(f.output, []); }
  finally { cleanup(f); }
});

test('prelaunch storage refusal sends no readiness and creates no native writer', async () => {
  const f = fixture({ preflightFailure: true });
  try { await assert.rejects(f.prepare(), error => error.code === 'synthetic_storage_refusal');
    assert.deepEqual(f.events, ['preflight']); assert.deepEqual(f.output, []); }
  finally { cleanup(f); }
});

test('readiness waits for an exact permit instead of importing Playwright', async () => {
  const f = fixture(); let result;
  try {
    let settled = false; const pending = f.prepare().then(value => { settled = true; return value; });
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(settled, false); assert.equal(f.output.length, 1);
    assert.deepEqual(JSON.parse(f.output[0].slice(prefix.length)), {
      kind: 'bamboo_browser_launch_ready', version: 1, attempt_id: attempt,
    });
    assert.deepEqual(f.events, ['preflight']); assert.equal(f.imports(), 0);
    f.input.write(grant()); result = await pending;
    assert.equal(result.environment.BAMBOO_BROWSER_STORAGE_GENERATION, generation);
    assert.equal(f.env.BAMBOO_BROWSER_STORAGE_GENERATION, undefined);
  } finally { cleanup(f, result); }
});

for (const [label, changed] of [
  ['wrong attempt', { attempt_id: generation }],
  ['wrong version', { version: 2 }],
  ['unknown proof field', { confirmed: true }],
  ['wrong kind', { kind: 'runtime_state' }],
  ['nil generation', { launch_generation: '00000000-0000-0000-0000-000000000000' }],
]) test(`prelaunch refuses ${label} without importing a native writer`, async () => {
  const f = fixture();
  try { const pending = f.prepare(); f.input.end(grant(changed));
    await assert.rejects(pending, error => error.code === 'browser_launch_admission_invalid'); }
  finally { cleanup(f); }
});

test('prelaunch EOF and duplicate fields are refusals, never permissions', async () => {
  for (const bytes of ['', prefix + `{"kind":"bamboo_browser_launch_grant","version":1,"attempt_id":"${attempt}","attempt_id":"${attempt}","launch_generation":"${generation}"}\n`]) {
    const f = fixture();
    try { const pending = f.prepare(); f.input.end(bytes);
      await assert.rejects(pending, error => error.code === 'browser_launch_admission_invalid'); }
    finally { cleanup(f); }
  }
});

test('permit and first runtime request in one chunk preserve the runtime request', async () => {
  const f = fixture(); let result;
  try {
    const pending = f.prepare(); const request = JSON.stringify({ id: 1, action: 'runtime_state', version: 2 });
    f.input.write(grant() + request + '\n'); result = await pending;
    assert.equal((await result.requests.next()).value, request);
  } finally { cleanup(f, result); }
});

test('oversized admission frame is refused rather than used as a permit', async () => {
  const f = fixture();
  try { const pending = f.prepare(); f.input.end('x'.repeat(16_385) + '\n');
    await assert.rejects(pending, error => error.code === 'browser_launch_admission_invalid'); }
  finally { cleanup(f); }
});

test('oversized admission without LF is stopped without waiting for more input', async () => {
  const f = fixture();
  try { const pending = f.prepare(); f.input.write('x'.repeat(16_385));
    await assert.rejects(pending, error => error.code === 'browser_launch_admission_invalid'); }
  finally { cleanup(f); }
});
