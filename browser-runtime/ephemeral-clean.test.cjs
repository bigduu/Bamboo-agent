'use strict';
// Pure production-policy/runtime checks. No Chromium, disk image or user profile.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const vm = require('node:vm');
const { EventEmitter } = require('node:events');
const { test } = require('node:test');

const source = fs.readFileSync(path.join(__dirname, 'host.cjs'), 'utf8');
function fixture(platform) {
  const scratch = platform === 'win32' ? 'C:\\synthetic\\owned-clean' : '/synthetic/owned-clean';
  const environment = { BAMBOO_BROWSER_HOST_PROTOCOL: '2', BAMBOO_BROWSER_EPHEMERAL_CLEAN: '1',
    TMPDIR: scratch, TEMP: scratch, TMP: scratch, BAMBOO_BROWSER_DOWNLOAD_ROOT: scratch };
  const filesystem = Object.create(fs);
  filesystem.lstatSync = target => {
    assert.equal(target, scratch, 'Only the owned temporary directory may be inspected');
    return { isDirectory: () => true, isSymbolicLink: () => false, uid: 501, mode: 0o40700 };
  };
  filesystem.realpathSync = target => target;
  const context = Object.assign(new EventEmitter(), { pages: () => [] });
  let createdContexts = 0;
  const sandbox = vm.createContext({
    require(name) {
      if (name === 'node:fs') return filesystem;
      if (name === 'node:path') return platform === 'win32' ? path.win32 : path.posix;
      if (name === 'node:child_process') return {
        execFileSync() { throw new Error('Clean admission must never query or mutate native storage'); },
      };
      return require(name);
    },
    module: { exports: {} }, process: { env: {}, platform, getuid: () => 501 },
    Buffer, URL, setTimeout, clearTimeout, setInterval, clearInterval,
    fixture: { context, cdp: Object.assign(new EventEmitter(), { send: async () => ({}) }),
      browser: { newContext() { createdContexts++; throw new Error('Unexpected context allocation'); } } },
  });
  new vm.Script(source).runInContext(sandbox);
  const api = vm.runInContext(`({
    settings: env => productionStorageSettings(env),
    prepare: env => prepareNativeLaunch(env),
    runtime: () => createHostRuntime({browser:fixture.browser,sharedContext:fixture.context,
      downloadCdp:fixture.cdp,contextOptions:{},cleanOnly:true}),
  })`, sandbox);
  return { api, environment, scratch, filesystem, sandbox, contexts: () => createdContexts };
}

for (const platform of ['linux', 'win32']) {
  test(`${platform} explicit production clean uses owned temporary storage without APFS`, () => {
    const f = fixture(platform);
    const settings = f.api.settings(f.environment);
    assert.equal(settings.cleanOnly, true);
    assert.equal(settings.scratch, f.scratch);
    assert.equal(settings.profile, null);
    assert.equal(settings.plan, undefined, 'No hard-quota claim for the legacy temporary provider');
  });
  test(`${platform} clean bootstrap does not request a persistent-writer permission`, async () => {
    const f = fixture(platform);
    assert.equal(await f.api.prepare(f.environment), null);
  });
  test(`${platform} persistent or ambiguous configuration cannot enter temporary clean`, () => {
    const f = fixture(platform);
    for (const name of ['BAMBOO_BROWSER_PROFILE_DIR', 'BAMBOO_BROWSER_STORAGE_DATA_ROOT',
      'BAMBOO_BROWSER_PROFILE_CONTROL_DIR', 'BAMBOO_BROWSER_STORAGE_GENERATION',
      'BAMBOO_BROWSER_STORAGE_VOLUME_UUID', 'BAMBOO_BROWSER_LAUNCH_ATTEMPT']) {
      assert.throws(() => f.api.settings({ ...f.environment, [name]: 'synthetic-conflicting-value' }),
        error => error.reason === 'browser_ephemeral_clean_configuration_invalid', name);
    }
    assert.throws(() => f.api.settings({ ...f.environment, TMP: 'other' }),
      error => error.reason === 'browser_ephemeral_clean_configuration_invalid');
  });
  test(`${platform} shared still refuses instead of silently selecting clean`, () => {
    const f = fixture(platform); delete f.environment.BAMBOO_BROWSER_EPHEMERAL_CLEAN;
    assert.throws(() => f.api.settings(f.environment),
      error => error.reason === 'browser_storage_provider_unsupported');
  });
}

test('macOS cannot use the temporary selector to bypass its bounded account provider', () => {
  const f = fixture('darwin');
  assert.throws(() => f.api.settings(f.environment),
    error => error.reason === 'browser_ephemeral_clean_provider_unsupported');
});

test('clean-only readiness advertises exactly clean and no persistent quota capability', async () => {
  const f = fixture('linux');
  const state = await f.api.runtime().handle({ action: 'runtime_state' });
  assert.deepEqual(Array.from(state.profile_modes), ['clean']);
  assert.deepEqual(JSON.parse(JSON.stringify(state.storage_boundary)),
    { provider: 'ephemeral_clean_v1', persistent: false });
});

test('clean-only runtime rejects explicit and implicit shared before allocating a context', async () => {
  const f = fixture('linux'); const runtime = f.api.runtime();
  const state = await runtime.handle({ action: 'runtime_state' });
  for (const args of [{ profile_mode: 'shared' }, {}]) {
    await assert.rejects(runtime.handle({ version: 2, host_generation: state.host_generation,
      scope_id: 'a'.repeat(24), scope_generation: 'b'.repeat(24), action: 'scope_open', args }),
    error => error.code === 'browser_storage_unavailable' && error.reason === 'browser_shared_profile_unsupported');
  }
  assert.equal(f.contexts(), 0);
  assert.equal((await runtime.handle({ action: 'runtime_state' })).scope_count, 0);
});

test('clean chats reuse one mock browser but own distinct contexts and close independently', async t => {
  const f = fixture('linux');
  const scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'bamboo-clean-policy-'));
  t.after(() => fs.rmSync(scratch, { recursive: true, force: true }));
  const contexts = [];
  const cdp = Object.assign(new EventEmitter(), { async send(method) {
    return method === 'Target.getBrowserContexts'
      ? { browserContextIds: contexts.filter(context => !context.closed).map(context => context.id) } : {};
  } });
  const browser = { async newContext() {
    const context = Object.assign(new EventEmitter(), { id: `synthetic-${contexts.length}`,
      closed: false, pages: () => [], async close() { this.closed = true; } });
    contexts.push(context); return context;
  } };
  f.sandbox.fixture = { scratch, browser, cdp, context: new EventEmitter() };
  const runtime = vm.runInContext(`
    downloadDir = fixture.scratch;
    configureContext = async () => {};
    createHostRuntime({browser:fixture.browser,sharedContext:fixture.context,
      downloadCdp:fixture.cdp,contextOptions:{},cleanOnly:true})`, f.sandbox);
  const state = await runtime.handle({ action: 'runtime_state' });
  const request = (scope, action, args = {}) => ({ version: 2, host_generation: state.host_generation,
    scope_id: scope.repeat(24), scope_generation: scope.repeat(24), action, args });
  const [a, b] = await Promise.all(['a', 'b'].map(id =>
    runtime.handle(request(id, 'scope_open', { profile_mode: 'clean' }))));
  assert.equal(a.profile_mode, 'clean'); assert.equal(b.profile_mode, 'clean');
  assert.equal(a.storage_shared, false); assert.equal(b.storage_shared, false);
  assert.equal(contexts.length, 2); assert.notEqual(contexts[0], contexts[1]);
  await runtime.handle(request('a', 'scope_close'));
  assert.equal(contexts[0].closed, true); assert.equal(contexts[1].closed, false);
  assert.equal((await runtime.handle(request('b', 'state'))).profile_mode, 'clean');
  await runtime.closeScopes();
  assert.equal(contexts[1].closed, true);
});
