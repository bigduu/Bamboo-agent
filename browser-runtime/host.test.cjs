const assert = require('node:assert/strict');
const http = require('node:http');
const { spawn } = require('node:child_process');
const readline = require('node:readline');
const { once } = require('node:events');
const { test } = require('node:test');
const path = require('node:path');

test('one isolated page supplies DOM, screenshot, and interactive changes without an iframe', async () => {
  const fixture = http.createServer((_request, response) => {
    response.writeHead(200, {
      'content-type': 'text/html; charset=utf-8',
      'x-frame-options': 'DENY',
      'content-security-policy': "frame-ancestors 'none'",
    });
    response.end('<button id="increment" onclick="const output=document.querySelector(\'output\');output.textContent=String(Number(output.textContent)+1)">Increment</button><input id="name" aria-label="Name"><output>0</output>');
  });
  fixture.listen(0, '127.0.0.1');
  await once(fixture, 'listening');
  const url = `http://127.0.0.1:${fixture.address().port}/`;
  const host = spawn(process.env.BAMBOO_BROWSER_NODE || process.execPath, [path.join(__dirname, 'host.cjs')], {
    env: process.env,
    stdio: ['pipe', 'pipe', 'inherit'],
  });
  const pending = new Map();
  let nextId = 1;
  let frames = 0;
  const lines = readline.createInterface({ input: host.stdout });
  lines.on('line', line => {
    const message = JSON.parse(line);
    if (message.event === 'frame') { frames++; return; }
    const resolve = pending.get(message.id);
    if (resolve) { pending.delete(message.id); resolve(message); }
  });
  const call = (action, args = {}) => new Promise((resolve, reject) => {
    const id = nextId++;
    const timeout = setTimeout(() => { pending.delete(id); reject(new Error(`${action} timed out`)); }, 30_000);
    pending.set(id, message => { clearTimeout(timeout); resolve(message); });
    host.stdin.write(`${JSON.stringify({ id, action, args })}\n`);
  });
  try {
    const initial = await call('state');
    assert.equal(initial.ok, true);
    const epoch = initial.result.page_epoch;
    const navigated = await call('navigate', { url, expected_epoch: epoch });
    assert.equal(navigated.ok, true);
    assert.notEqual(navigated.result.page_epoch, epoch);
    const pageEpoch = navigated.result.page_epoch;
    const before = await call('dom');
    assert.match(before.result.snapshot, /Increment/);
    assert.match(before.result.html, /<output>0<\/output>/);
    const clicked = await call('click_selector', { selector: '#increment', expected_epoch: pageEpoch });
    assert.equal(clicked.ok, true);
    const uiClick = await call('input', { kind: 'click', x: 40, y: 20, expected_epoch: pageEpoch });
    assert.equal(uiClick.ok, true);
    const filled = await call('fill_selector', { selector: '#name', text: 'Lotus', expected_epoch: pageEpoch });
    assert.equal(filled.ok, true);
    const after = await call('dom');
    assert.match(after.result.html, /<output>2<\/output>/);
    assert.match(after.result.snapshot, /Lotus/);
    const screenshot = await call('screenshot');
    assert.equal(screenshot.ok, true);
    assert.ok(Buffer.from(screenshot.result.data, 'base64').length > 1000);
    assert.equal(screenshot.result.page_epoch, pageEpoch);
    assert.ok(frames > 0);
    const resized = await call('viewport', { width: 640, height: 480, expected_epoch: pageEpoch });
    assert.equal(resized.ok, true);
    assert.notEqual(resized.result.page_epoch, pageEpoch);
    assert.deepEqual(resized.result.viewport, { width: 640, height: 480 });
    const stale = await call('input', { kind: 'key', key: 'Enter', expected_epoch: epoch });
    assert.equal(stale.code, 'stale_epoch');
    const oldFrame = await call('input', { kind: 'click', x: 40, y: 20, expected_epoch: pageEpoch });
    assert.equal(oldFrame.code, 'stale_epoch');
    const unsafe = await call('navigate', { url: 'file:///tmp/secret', expected_epoch: resized.result.page_epoch });
    assert.equal(unsafe.code, 'invalid_url');
  } finally {
    host.stdin.end();
    host.kill();
    fixture.close();
    await once(fixture, 'close');
  }
});
