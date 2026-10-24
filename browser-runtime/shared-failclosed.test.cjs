'use strict';
// Actual runtime code with a deterministic failed CDP policy restore only.
const assert = require('node:assert/strict');
const { test } = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const vm = require('node:vm');
const { EventEmitter } = require('node:events');

test('failed download-policy restore irreversibly fences all new shared-runtime work', async () => {
  const calls = [];
  const cdp = { on() {}, async send(method, args) {
    calls.push([method, args]);
    if (args?.behavior === 'deny') throw new Error('synthetic policy restore failure');
    return {};
  } };
  const sandbox = vm.createContext({
    require(name) { return name === 'playwright-core' ? { chromium: {} } : require(name); },
    module: { exports: {} }, process: { env: {}, platform: process.platform },
    Buffer, URL, setTimeout, clearTimeout, setInterval, clearInterval,
    fixture: { cdp, context: { on() {} }, browser: {} },
  });
  new vm.Script(fs.readFileSync(path.join(__dirname, 'host.cjs'), 'utf8')).runInContext(sandbox);
  const api = vm.runInContext(`
    const owned = createHostRuntime({browser:fixture.browser,sharedContext:fixture.context,
      downloadCdp:fixture.cdp,contextOptions:{}});
    const scope = {isClosed:()=>false,downloadDir:'/synthetic-only/no-filesystem-access',downloadContextId:undefined};
    ({ run:()=>owned.withDownload(scope,async()=>true),
       open:()=>owned.handle({action:'scope_open'}),
       retired:()=>retireAfterReply });`, sandbox);
  await assert.rejects(api.run(), error => error.code === 'browser_unavailable');
  assert.equal(api.retired(), true);
  await assert.rejects(api.open(), error => error.code === 'browser_unavailable');
  const count = calls.length;
  await assert.rejects(api.run(), error => error.code === 'browser_scope_closed');
  assert.equal(calls.length, count, 'fenced runtime cannot issue another native policy mutation');
  assert.deepEqual(calls.map(([, args]) => args.behavior), ['allowAndName', 'deny']);
});

test('normal backpressure replies preserve the complete v2 owner tuple', () => {
  const sandbox = vm.createContext({
    require(name) { return name === 'playwright-core' ? { chromium: {} } : require(name); },
    module: { exports: {} }, process: { env: {}, platform: process.platform },
    Buffer, URL, setTimeout, clearTimeout, setInterval, clearInterval,
  });
  new vm.Script(fs.readFileSync(path.join(__dirname, 'host.cjs'), 'utf8')).runInContext(sandbox);
  const result = vm.runInContext(`replyEnvelope({id:65,scope_id:'a'.repeat(24),scope_generation:'b'.repeat(24)},
    {ok:false,code:'invalid_request',error:'browser request capacity unavailable'},true,'c'.repeat(24))`, sandbox);
  assert.deepEqual(JSON.parse(JSON.stringify(result)), {
    id: 65, ok: false, code: 'invalid_request', error: 'browser request capacity unavailable',
    version: 2, host_generation: 'c'.repeat(24), scope_id: 'a'.repeat(24), scope_generation: 'b'.repeat(24),
  });
});

test('a native mutation queued before policy failure cannot escape the runtime fence', async t => {
  const scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'bamboo-queued-policy-'));
  t.after(() => fs.rmSync(scratch, { recursive: true, force: true }));
  let release;
  const gate = new Promise(resolve => { release = resolve; });
  let navigations = 0;
  const cdp = Object.assign(new EventEmitter(), { async send(method, args) {
    if (method === 'Browser.setDownloadBehavior' && args.behavior === 'deny') throw new Error('synthetic deny failure');
    return { currentIndex: 0, entries: [] };
  } });
  const context = Object.assign(new EventEmitter(), { async newCDPSession() { return cdp; } });
  let pageClosed = false;
  const page = Object.assign(new EventEmitter(), {
    setDefaultTimeout() {}, url: () => 'about:blank', isClosed: () => pageClosed,
    viewportSize: () => ({ width: 1000, height: 720 }), title: async () => 'Synthetic page',
    async goto() { navigations++; },
    async close() { pageClosed = true; page.emit('close'); },
  });
  const sandbox = vm.createContext({
    require(name) { return name === 'playwright-core' ? { chromium: {} } : require(name); },
    module: { exports: {} }, process: { env: {}, platform: process.platform },
    Buffer, URL, setTimeout, clearTimeout, setInterval, clearInterval,
    fixture: { cdp, context, page, gate, scratch },
  });
  new vm.Script(fs.readFileSync(path.join(__dirname, 'host.cjs'), 'utf8')).runInContext(sandbox);
  const api = vm.runInContext(`
    const runtime = createHostRuntime({browser:{},sharedContext:fixture.context,downloadCdp:fixture.cdp,contextOptions:{}});
    const download = runtime.withDownload;
    runtime.withDownload = (scope) => download(scope,async()=>{await fixture.gate;return {};});
    const scope = createChatScope({id:'a'.repeat(24),generation:'b'.repeat(24),profileMode:'shared',
      context:fixture.context,downloadDir:fixture.scratch,runtime});
    scope.adoptPage(fixture.page);
    ({state:()=>scope.invoke('state',{}),run:()=>scope.invoke('download',{}),
      queue:epoch=>scope.invoke('navigate',{url:'https://synthetic.invalid',expected_epoch:epoch}),
      close:()=>scope.close()});`, sandbox);
  const state = await api.state();
  const failed = api.run();
  const queued = api.queue(state.page_epoch);
  const observed = Promise.allSettled([failed, queued]);
  release();
  const results = await observed;
  await api.close();
  assert.equal(results[0].status, 'rejected');
  assert.equal(results[1].status, 'rejected');
  assert.equal(results[1].reason.code, 'browser_unavailable');
  assert.equal(navigations, 0, 'queued work must not issue a native page navigation after the fence');
});

test('ordinary backpressure preserves all chat cleanup slots without retiring the host', () => {
  const sandbox = vm.createContext({
    require(name) { return name === 'playwright-core' ? { chromium: {} } : require(name); },
    module: { exports: {} }, process: { env: {}, platform: process.platform },
    Buffer, URL, setTimeout, clearTimeout, setInterval, clearInterval,
  });
  new vm.Script(fs.readFileSync(path.join(__dirname, 'host.cjs'), 'utf8')).runInContext(sandbox);
  const api = vm.runInContext(`({ budget:createRequestBudget(), retired:()=>retireAfterReply,
    cleanup:request=>isScopeCleanup(request,'a'.repeat(24)) })`, sandbox);
  const ordinary = Array.from({ length: 48 }, () => api.budget.acquire(false));
  assert.throws(() => api.budget.acquire(false), error => error.code === 'browser_busy');
  const cleanup = Array.from({ length: 16 }, () => api.budget.acquire(true));
  assert.throws(() => api.budget.acquire(true), error => error.code === 'browser_busy');
  assert.equal(api.retired(), false, 'known backpressure cannot retire a healthy browser');
  ordinary.forEach(release => { release(); release(); });
  const later = Array.from({ length: 48 }, () => api.budget.acquire(false));
  cleanup.forEach(release => release()); later.forEach(release => release());
  const request = { action: 'scope_close', host_generation: 'a'.repeat(24), scope_id: 'b'.repeat(24), scope_generation: 'c'.repeat(24) };
  assert.equal(api.cleanup(request), true);
  for (const invalid of [{ ...request, action: 'state' }, { ...request, host_generation: 'd'.repeat(24) },
    { ...request, scope_id: 'other' }, { ...request, scope_generation: null }]) assert.equal(api.cleanup(invalid), false);
});
