'use strict';
const assert = require('node:assert/strict');
const { test } = require('node:test');
const { spawn } = require('node:child_process');
const { once, EventEmitter } = require('node:events');
const { randomBytes, createHash } = require('node:crypto');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const readline = require('node:readline');
const http = require('node:http');

async function fixture(t) {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'bamboo-multi-chat-host-')));
  const profile = path.join(root, createHash('sha256').update('synthetic-shared-account').digest('hex'));
  const home = path.join(root, 'home');
  fs.mkdirSync(profile, { mode: 0o700 }); fs.mkdirSync(home, { mode: 0o700 });
  let heldNavigation;
  let navigationEntered;
  const heldNavigationEntered = new Promise(resolve => { navigationEntered = resolve; });
  const releaseNavigation = () => heldNavigation?.end('<title>Held navigation</title>');
  const server = http.createServer((request, response) => {
    if (request.url === '/held') { heldNavigation = response; navigationEntered(); return; }
    if (request.url === '/slow') { setTimeout(() => response.end('<title>Slow A</title>'), 350); return; }
    if (request.url === '/download') {
      response.setHeader('content-type', 'application/octet-stream');
      response.setHeader('content-disposition', 'attachment; filename="synthetic.txt"');
      setTimeout(() => response.end('synthetic shared download'), 150); return;
    }
    response.setHeader('content-type', 'text/html');
    response.end('<!doctype html><title>Synthetic shared</title><button onclick="alert(\'A only\')">Dialog</button><a id="download" href="/download">Download</a>');
  });
  server.listen(0, '127.0.0.1'); await once(server, 'listening');
  const url = `http://127.0.0.1:${server.address().port}`;
  // These are actual Chromium page/routing component tests with mocked native
  // storage admission, not quota or production-provider acceptance.
  const child = spawn(process.execPath, [path.join(__dirname, 'test-host-fixture.cjs')], {
    env: { PATH: process.env.PATH, NODE_PATH: process.env.NODE_PATH || '', HOME: home,
      TMPDIR: root, TEMP: root, TMP: root, BAMBOO_BROWSER_DOWNLOAD_ROOT: root,
      BAMBOO_BROWSER_PROFILE_DIR: profile, BAMBOO_BROWSER_HOST_PROTOCOL: '2',
      ...(process.env.BAMBOO_BROWSER_EXECUTABLE ? { BAMBOO_BROWSER_EXECUTABLE: process.env.BAMBOO_BROWSER_EXECUTABLE } : {}),
      ...(process.env.PLAYWRIGHT_BROWSERS_PATH ? { PLAYWRIGHT_BROWSERS_PATH: process.env.PLAYWRIGHT_BROWSERS_PATH } : {}),
    }, stdio: ['pipe', 'pipe', 'pipe', 'ipc'],
  });
  const pending = new Map(); const controls = new Map(); const notifications = new EventEmitter();
  const events = []; const calls = []; let next = 1; let generation; let stderr = '';
  child.stderr.on('data', bytes => { stderr = (stderr + bytes).slice(-5000); });
  const exited = once(child, 'exit').then(([code, signal]) => {
    for (const waiter of pending.values()) { clearTimeout(waiter.timer); waiter.reject(new Error(`host exit ${code}/${signal}: ${stderr}`)); }
    for (const waiter of controls.values()) { clearTimeout(waiter.timer); waiter.reject(new Error(`fixture exit ${code}/${signal}`)); }
    pending.clear(); controls.clear(); return { code, signal };
  });
  child.on('message', message => {
    const key = `${message.token}:${message.type}`;
    const waiter = controls.get(key);
    if (waiter) { controls.delete(key); clearTimeout(waiter.timer); waiter.resolve(); }
  });
  const control = (type, token) => new Promise((resolve, reject) => {
    const key = `${token}:${type}`;
    const timer = setTimeout(() => { controls.delete(key); reject(new Error(`${type} fixture handshake timeout`)); }, 15000);
    controls.set(key, { resolve, reject, timer });
  });
  readline.createInterface({ input: child.stdout }).on('line', line => {
    const value = JSON.parse(line);
    if (value.event) { events.push(value); notifications.emit('host-event', value); return; }
    const waiter = pending.get(value.id); if (!waiter) return;
    pending.delete(value.id); clearTimeout(waiter.timer);
    try {
      assert.equal(value.version, 2);
      if (generation) assert.equal(value.host_generation, generation);
      if (waiter.owner) {
        assert.equal(value.scope_id, waiter.owner.id);
        assert.equal(value.scope_generation, waiter.owner.generation);
      }
    } catch (error) { waiter.reject(error); return; }
    if (value.ok) waiter.resolve(value.result);
    else waiter.reject(Object.assign(new Error(value.error), { code: value.code }));
  });
  const call = (action, args = {}, owner) => new Promise((resolve, reject) => {
    calls.push({ action, args, owner });
    const id = next++;
    const timer = setTimeout(() => { pending.delete(id); reject(new Error(`${action} timeout: ${stderr}`)); }, 15000);
    pending.set(id, { resolve, reject, timer, owner });
    child.stdin.write(JSON.stringify({ version: 2, id, action, args,
      ...(generation ? { host_generation: generation } : {}),
      ...(owner ? { scope_id: owner.id, scope_generation: owner.generation } : {}),
    }) + '\n');
  });
  t.after(async () => {
    releaseNavigation();
    if (child.connected) child.disconnect();
    child.stdin.end();
    const result = await exited;
    server.closeAllConnections(); await new Promise(resolve => server.close(resolve));
    // All files are synthetic. Retain them on an abnormal/unknown native exit.
    if (result.code === 0) fs.rmSync(root, { recursive: true, force: true });
  });
  const info = await call('runtime_state'); generation = info.host_generation;
  assert.equal(info.protocol_version, 2);
  const open = async (mode = 'shared', owner = { id: randomBytes(12).toString('hex'), generation: randomBytes(12).toString('hex') }) => {
    await call('scope_open', { profile_mode: mode }, owner);
    return { ...owner,
      state: () => call('state', {}, owner),
      async visit(target = url) { const state = await this.state(); return call('navigate', { url: target, expected_epoch: state.page_epoch }, owner); },
      async evaluate(code) { const state = await this.state(); return (await call('eval', { code, expected_epoch: state.page_epoch, expected_url: state.url }, owner)).value; },
      close: () => call('scope_close', {}, owner),
    };
  };
  const holdRead = async (action, owner, targetUrl) => {
    const token = randomBytes(12).toString('hex');
    const armed = control('read-armed', token);
    child.send({ type: 'hold-read', token, url: targetUrl });
    await armed;
    const entered = control('read-entered', token);
    const read = call(action, {}, owner);
    await Promise.race([entered, read.then(() => { throw new Error('Read returned before its CDP barrier'); })]);
    return { read, async release() {
      const released = control('read-released', token);
      child.send({ type: 'release-read', token });
      await released;
    } };
  };
  const waitEvent = predicate => {
    const existing = events.find(predicate);
    if (existing) return Promise.resolve(existing);
    return new Promise((resolve, reject) => {
      const finish = value => { if (predicate(value)) { clearTimeout(timer); notifications.off('host-event', finish); resolve(value); } };
      const timer = setTimeout(() => { notifications.off('host-event', finish); reject(new Error('host event timeout')); }, 15000);
      notifications.on('host-event', finish);
    });
  };
  return { info, call, open, url, events, profile, calls, holdRead, waitEvent,
    heldNavigationEntered, releaseNavigation };
}

for (const action of ['state', 'tab_list']) {
  test(`read admission: held ${action} permits one viewport write without affecting another scope`, async t => {
    const f = await fixture(t); const a = await f.open(); const b = await f.open();
    const initial = await a.visit(f.url + '/?scope=a');
    const other = await b.visit(f.url + '/?scope=b');
    const barrier = await f.holdRead(action, a, initial.url);
    let readSettled = false;
    void barrier.read.then(() => { readSettled = true; });
    let resized; let read;
    try {
      resized = await f.call('viewport', { width: 519, height: 737, expected_epoch: initial.page_epoch }, a);
      assert.deepEqual(resized.viewport, { width: 519, height: 737 });
      assert.equal(readSettled, false, 'the one write must finish while the ordinary read is still held');
      assert.deepEqual((await b.state()).viewport, other.viewport);
      assert.equal((await b.state()).active_tab_id, other.active_tab_id);
    } finally { await barrier.release(); read = await barrier.read; }
    assert.deepEqual(read.viewport, resized.viewport);
    assert.equal(read.page_epoch, resized.page_epoch);
    assert.equal(read.active_tab_id, resized.active_tab_id);
    const frame = await f.waitEvent(event => event.event === 'frame' && event.scope_id === a.id &&
      event.page_epoch === resized.page_epoch && event.viewport_width === 519 && event.viewport_height === 737);
    assert.equal(frame.scope_generation, a.generation);
    assert.equal(frame.active_tab_id, resized.active_tab_id);
    assert.equal(f.calls.filter(call => call.action === 'viewport' && call.owner === a).length, 1);
  });
}

test('read admission: state and tab_list preserve real dialog identities and blocked writes', async t => {
  const f = await fixture(t); const a = await f.open(); const b = await f.open();
  await Promise.all([a.visit(), b.visit()]);
  for (const type of ['alert', 'confirm', 'prompt']) {
    const initial = await a.state();
    const shown = await f.call('eval', { code: `(() => { ${type}('A only'); return true; })()`, expected_epoch: initial.page_epoch, expected_url: initial.url }, a);
    const dialog = shown.pending_dialog;
    assert.equal(dialog.type, type);
    for (const action of ['state', 'tab_list']) {
      const observed = await f.call(action, {}, a);
      assert.equal(observed.pending_dialog.dialog_id, dialog.dialog_id);
      assert.equal(observed.pending_dialog.page_epoch, dialog.page_epoch);
    }
    assert.equal((await b.state()).pending_dialog, undefined);
    await assert.rejects(f.call('viewport', { width: 519, height: 737, expected_epoch: shown.page_epoch }, a),
      error => error.code === 'dialog_pending');
    await assert.rejects(f.call('dialog_respond', { dialog_id: '0'.repeat(24), accept: true, expected_epoch: shown.page_epoch }, a),
      error => error.code === 'stale_dialog');
    await assert.rejects(f.call('dialog_respond', { dialog_id: dialog.dialog_id, accept: true, expected_epoch: shown.page_epoch - 1 }, a),
      error => error.code === 'stale_epoch');
    const answered = await f.call('dialog_respond', { dialog_id: dialog.dialog_id, accept: true, expected_epoch: shown.page_epoch }, a);
    assert.equal(answered.pending_dialog, undefined);
  }
});

test('read admission: a second mutation stays queued and retains its stale epoch failure', async t => {
  const f = await fixture(t); const a = await f.open(); const initial = await a.visit();
  const navigation = f.call('navigate', { url: f.url + '/held', expected_epoch: initial.page_epoch }, a);
  await f.heldNavigationEntered;
  let writeSettled = false;
  const write = f.call('viewport', { width: 519, height: 737, expected_epoch: initial.page_epoch }, a)
    .then(value => ({ value }), error => ({ error })).then(result => { writeSettled = true; return result; });
  const during = await a.state();
  assert.equal(writeSettled, false, 'the second mutation must remain behind navigation');
  assert.deepEqual(during.viewport, initial.viewport);
  f.releaseNavigation();
  const navigated = await navigation;
  assert.equal((await write).error.code, 'stale_epoch');
  const final = await a.state();
  assert.equal(final.page_epoch, navigated.page_epoch);
  assert.deepEqual(final.viewport, initial.viewport);
  assert.equal(f.calls.filter(call => call.action === 'viewport').length, 1);
});

test('one host routes shared chat pages independently and preserves the account profile on chat close', async t => {
  const f = await fixture(t); const a = await f.open(); const b = await f.open();
  const [sa, sb] = await Promise.all([a.visit(), b.visit()]);
  assert.notEqual(sa.active_tab_id, sb.active_tab_id);
  await a.evaluate('localStorage.setItem("shared-fixture", "yes"); document.cookie="shared-fixture=yes; Path=/"; true');
  assert.deepEqual(await b.evaluate('({local:localStorage.getItem("shared-fixture"),cookie:document.cookie})'),
    { local: 'yes', cookie: 'shared-fixture=yes' });
  await assert.rejects(f.call('tab_activate', { tab_id: sb.active_tab_id, expected_epoch: (await a.state()).page_epoch }, a),
    error => error.code === 'invalid_request');
  await a.close(); assert.ok(fs.existsSync(f.profile));
  assert.equal((await b.state()).active_tab_id, sb.active_tab_id);
  assert.equal((await f.call('runtime_state')).browser_instance_id, f.info.browser_instance_id);
  assert.equal(await b.evaluate('localStorage.getItem("shared-fixture")'), 'yes');
  const frames = f.events.filter(event => event.event === 'frame' || event.event === 'frame_reset');
  assert.ok(frames.some(event => event.scope_id === a.id));
  assert.ok(frames.some(event => event.scope_id === b.id));
  assert.ok(frames.every(event => event.scope_id === a.id && event.scope_generation === a.generation ||
    event.scope_id === b.id && event.scope_generation === b.generation));
});

test('clean scopes use the same browser but inherit neither shared nor previous clean state', async t => {
  const f = await fixture(t); const shared = await f.open(); await shared.visit();
  await shared.evaluate('localStorage.setItem("shared-fixture","shared"); document.cookie="fixture=shared; Path=/"; true');
  for (let i = 0; i < 2; i++) {
    const clean = await f.open('clean'); await clean.visit();
    assert.deepEqual(await clean.evaluate('({local:localStorage.getItem("shared-fixture"),cookie:document.cookie})'),
      { local: null, cookie: '' });
    await clean.evaluate('localStorage.setItem("shared-fixture","clean"); document.cookie="fixture=clean; Path=/"; true');
    await clean.close();
  }
  assert.equal(await shared.evaluate('localStorage.getItem("shared-fixture")'), 'shared');
  assert.equal((await f.call('runtime_state')).browser_instance_id, f.info.browser_instance_id);
  assert.equal((await f.call('runtime_state')).scope_count, 1);
});

test('parallel navigation and a chat dialog do not block another chat or revive a closed generation', async t => {
  const f = await fixture(t); const a = await f.open(); const b = await f.open(); await b.visit();
  const slow = a.visit(f.url + '/slow');
  const fast = await b.evaluate('document.title'); assert.equal(fast, 'Synthetic shared'); await slow;
  await a.visit();
  const state = await a.state();
  const dialog = await f.call('click_selector', { selector: 'button', expected_epoch: state.page_epoch }, a);
  assert.ok(dialog.pending_dialog);
  assert.equal(await b.evaluate('document.title'), 'Synthetic shared');
  await a.close();
  await assert.rejects(f.call('state', {}, a), error => error.code === 'browser_scope_closed');
  await assert.rejects(f.call('scope_open', { profile_mode: 'shared' }, a), error => error.code === 'browser_scope_closed');
  assert.equal((await b.state()).url, f.url + '/');
});

test('shared popups inherit their opener and a mode conflict cannot alter a live clean scope', async t => {
  const f = await fixture(t); const a = await f.open(); const b = await f.open(); const clean = await f.open('clean');
  await Promise.all([a.visit(), b.visit(), clean.visit()]);
  const bTab = (await b.state()).active_tab_id;
  await a.evaluate('window.open(location.origin + "/popup"); true').catch(error => {
    if (error.code !== 'stale_epoch') throw error;
  });
  let state;
  for (let attempt = 0; attempt < 40; attempt++) {
    state = await a.state(); if (state.tabs.length === 2) break;
    await new Promise(resolve => setTimeout(resolve, 25));
  }
  assert.equal(state.tabs.length, 2);
  assert.equal((await b.state()).tabs.length, 1);
  assert.equal((await b.state()).active_tab_id, bTab);
  await assert.rejects(f.call('scope_open', { profile_mode: 'shared' }, clean),
    error => error.code === 'browser_profile_mode_conflict');
  assert.equal((await f.call('scope_open', {}, clean)).profile_mode, 'clean');
});

test('shared download arbitration preserves per-chat page and byte ownership', async t => {
  const f = await fixture(t); const a = await f.open(); const b = await f.open();
  await Promise.all([a.visit(), b.visit()]);
  const aState = await a.state(); const bState = await b.state();
  const first = f.call('download', { selector: '#download', expected_epoch: aState.page_epoch }, a);
  const second = f.call('download', { selector: '#download', expected_epoch: bState.page_epoch }, b);
  const results = await Promise.allSettled([first, second]);
  assert.equal(results[0].status, 'fulfilled', JSON.stringify(results.map(result =>
    ({ status: result.status, code: result.reason?.code, message: result.reason?.message }))));
  assert.equal(results[1].status, 'rejected');
  assert.equal(results[1].reason.code, 'browser_busy');
  assert.equal(Buffer.from(results[0].value.data_base64, 'base64').toString(), 'synthetic shared download');
  assert.equal((await a.state()).active_tab_id, aState.active_tab_id);
  assert.equal((await b.state()).active_tab_id, bState.active_tab_id);
  const later = await f.call('download', { selector: '#download', expected_epoch: (await b.state()).page_epoch }, b);
  assert.equal(Buffer.from(later.data_base64, 'base64').toString(), 'synthetic shared download');
});
