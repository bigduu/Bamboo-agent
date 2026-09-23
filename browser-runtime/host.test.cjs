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
    response.end('<style>button,input,output,select{display:block}</style><button id="increment" onclick="const output=document.querySelector(\'output\');output.textContent=String(Number(output.textContent)+1)">Increment</button><input id="name" aria-label="Name"><output>0</output><select id="single" onchange="document.querySelector(\'#chosen\').textContent=\'Single \'+this.value"><option value="">None</option><option value="red">Red</option></select><select id="multi" multiple onchange="document.querySelector(\'#chosen\').textContent=\'Multi \'+Array.from(this.selectedOptions).map(option=>option.value).join(\',\')"><option value="green">Green</option><option value="blue">Blue</option><option value="yellow">Yellow</option></select><output id="chosen">No selection</output>');
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
    const single = await call('select_option', { selector: '#single', values: ['red'], expected_epoch: pageEpoch });
    assert.equal(single.ok, true);
    assert.deepEqual(single.result.selected_values, ['red']);
    assert.equal(single.result.page_epoch, pageEpoch);
    const cleared = await call('select_option', { selector: '#single', values: [''], expected_epoch: pageEpoch });
    assert.equal(cleared.ok, true);
    assert.deepEqual(cleared.result.selected_values, ['']);
    const multiple = await call('select_option', { selector: '#multi', values: ['green', 'blue'], expected_epoch: pageEpoch });
    assert.equal(multiple.ok, true);
    assert.deepEqual(multiple.result.selected_values, ['green', 'blue']);
    assert.equal((await call('select_option', { selector: 'select', values: ['red'], expected_epoch: pageEpoch })).code, 'ambiguous_target');
    assert.equal((await call('select_option', { selector: '#name', values: ['red'], expected_epoch: pageEpoch })).code, 'invalid_target');
    assert.equal((await call('select_option', { selector: '#single', values: ['red', 'blue'], expected_epoch: pageEpoch })).code, 'invalid_target');
    assert.equal((await call('select_option', { selector: '#single', values: [], expected_epoch: pageEpoch })).code, 'invalid_target');
    assert.equal((await call('select_option', { selector: '#single', values: Array(17).fill('red'), expected_epoch: pageEpoch })).code, 'invalid_target');
    assert.equal((await call('select_option', { selector: '#single', values: ['x'.repeat(513)], expected_epoch: pageEpoch })).code, 'invalid_target');
    const unavailable = await call('select_option', { selector: '#single', values: ['private-option-not-present'], expected_epoch: pageEpoch });
    assert.equal(unavailable.code, 'selection_failed');
    assert.doesNotMatch(unavailable.error, /private-option-not-present/);
    const after = await call('dom');
    assert.match(after.result.html, /<output>2<\/output>/);
    assert.match(after.result.snapshot, /Lotus/);
    assert.match(after.result.html, /Multi green,blue/);
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
    assert.equal((await call('select_option', { selector: '#single', values: [''], expected_epoch: pageEpoch })).code, 'stale_epoch');
    const unsafe = await call('navigate', { url: 'file:///tmp/secret', expected_epoch: resized.result.page_epoch });
    assert.equal(unsafe.code, 'invalid_url');
  } finally {
    host.stdin.end();
    host.kill();
    fixture.close();
    await once(fixture, 'close');
  }
});

test('popup and explicit tabs keep active DOM, frames, and epochs on one page', async () => {
  const fixture = http.createServer((request, response) => {
    response.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
    if (request.url === '/one') {
      response.end('<title>One</title><button id="popup" onclick="window.open(\'/two\', \'_blank\')">Open Two</button><main>One page</main>');
    } else if (request.url === '/two') {
      response.end('<title>Two</title><main>Two page</main>');
    } else {
      response.end('<title>Three</title><main>Three page</main>');
    }
  });
  fixture.listen(0, '127.0.0.1');
  await once(fixture, 'listening');
  const base = 'http://127.0.0.1:' + fixture.address().port;
  const host = spawn(process.env.BAMBOO_BROWSER_NODE || process.execPath, [path.join(__dirname, 'host.cjs')], {
    env: process.env,
    stdio: ['pipe', 'pipe', 'inherit'],
  });
  const pending = new Map();
  const frames = [];
  let nextId = 1;
  const lines = readline.createInterface({ input: host.stdout });
  lines.on('line', line => {
    const message = JSON.parse(line);
    if (message.event === 'frame') { frames.push(message); return; }
    if (message.event === 'frame_reset') return;
    const resolve = pending.get(message.id);
    if (resolve) { pending.delete(message.id); resolve(message); }
  });
  const call = (action, args = {}) => new Promise((resolve, reject) => {
    const id = nextId++;
    const timeout = setTimeout(() => { pending.delete(id); reject(new Error(action + ' timed out')); }, 30_000);
    pending.set(id, message => { clearTimeout(timeout); resolve(message); });
    host.stdin.write(JSON.stringify({ id, action, args }) + '\n');
  });
  const waitForFrame = async (tabId, after, epoch) => {
    for (let attempt = 0; attempt < 100; attempt++) {
      const frame = frames.find(frame => frame.active_tab_id === tabId &&
        frame.frame_seq > after && frame.page_epoch === epoch);
      if (frame) return frame;
      await new Promise(resolve => setTimeout(resolve, 50));
    }
    throw new Error('no active frame for tab ' + tabId);
  };
  try {
    const initial = (await call('state')).result;
    assert.equal(initial.tabs.length, 1);
    assert.equal((await call('tab_activate', {
      tab_id: 'A'.repeat(10000), expected_epoch: initial.page_epoch,
    })).code, 'invalid_request');
    assert.equal((await call('tab_close', {
      tab_id: 'A'.repeat(24), expected_epoch: initial.page_epoch,
    })).code, 'invalid_request');
    const firstId = initial.active_tab_id;
    assert.equal(initial.tabs[0].tab_id, firstId);
    const first = (await call('navigate', { url: base + '/one', expected_epoch: initial.page_epoch })).result;
    const firstFrame = await waitForFrame(firstId, 0, first.page_epoch);
    assert.equal(firstFrame.page_epoch, first.page_epoch);
    const popup = await call('click_selector', { selector: '#popup', expected_epoch: first.page_epoch });
    assert.equal(popup.ok, true);
    let popupState = popup.result;
    for (let attempt = 0; popupState.tabs.length < 2 && attempt < 20; attempt++) {
      await new Promise(resolve => setTimeout(resolve, 50));
      popupState = (await call('state')).result;
    }
    assert.equal(popupState.tabs.length, 2);
    assert.notEqual(popupState.active_tab_id, firstId);
    assert.notEqual(popupState.page_epoch, first.page_epoch);
    const popupId = popupState.active_tab_id;
    const popupDom = (await call('dom')).result;
    assert.equal(popupDom.active_tab_id, popupId);
    assert.match(popupDom.html, /Two page/);
    assert.doesNotMatch(popupDom.html, /One page/);
    const popupFrame = await waitForFrame(popupId, firstFrame.frame_seq, popupState.page_epoch);
    assert.equal(popupFrame.page_epoch, popupState.page_epoch);
    assert.ok(Buffer.from(popupFrame.data, 'base64').length > 1000);
    assert.equal((await call('input', { kind: 'key', key: 'Enter', expected_epoch: first.page_epoch })).code, 'stale_epoch');

    const activated = (await call('tab_activate', { tab_id: firstId, expected_epoch: popupState.page_epoch })).result;
    assert.equal(activated.active_tab_id, firstId);
    assert.notEqual(activated.page_epoch, popupState.page_epoch);
    assert.match((await call('dom')).result.html, /One page/);
    assert.equal((await call('screenshot')).result.active_tab_id, firstId);
    const firstAgainFrame = await waitForFrame(firstId, popupFrame.frame_seq, activated.page_epoch);
    assert.equal(firstAgainFrame.page_epoch, activated.page_epoch);

    const closedInactive = (await call('tab_close', { tab_id: popupId, expected_epoch: activated.page_epoch })).result;
    assert.equal(closedInactive.tabs.length, 1);
    assert.equal(closedInactive.page_epoch, activated.page_epoch);
    const created = (await call('tab_create', { expected_epoch: closedInactive.page_epoch })).result;
    assert.equal(created.tabs.length, 2);
    assert.notEqual(created.active_tab_id, firstId);
    assert.notEqual(created.page_epoch, closedInactive.page_epoch);
    const thirdId = created.active_tab_id;
    const third = (await call('navigate', { url: base + '/three', expected_epoch: created.page_epoch })).result;
    assert.equal(third.active_tab_id, thirdId);
    assert.match((await call('dom')).result.html, /Three page/);
    const thirdFrame = await waitForFrame(thirdId, firstAgainFrame.frame_seq, third.page_epoch);
    assert.equal(thirdFrame.page_epoch, third.page_epoch);
    assert.equal((await call('navigate', { url: 'file:///tmp/secret', expected_epoch: third.page_epoch })).code, 'invalid_url');

    // Queued stop/start operations from older activations must not interrupt
    // the final active tab's capture or emit frames for another tab afterward.
    let switching = third;
    for (let count = 0; count < 12; count++) {
      const nextTabId = switching.active_tab_id === firstId ? thirdId : firstId;
      switching = (await call('tab_activate', {
        tab_id: nextTabId, expected_epoch: switching.page_epoch,
      })).result;
    }
    assert.equal(switching.active_tab_id, thirdId);
    const afterSwitch = frames.at(-1)?.frame_seq || 0;
    const subsequentFrames = frames.length;
    await waitForFrame(thirdId, afterSwitch, switching.page_epoch);
    await new Promise(resolve => setTimeout(resolve, 250));
    assert.ok(frames.slice(subsequentFrames).every(frame =>
      frame.active_tab_id === thirdId && frame.page_epoch === switching.page_epoch));

    const closedActive = (await call('tab_close', { tab_id: thirdId, expected_epoch: switching.page_epoch })).result;
    assert.equal(closedActive.active_tab_id, firstId);
    assert.notEqual(closedActive.page_epoch, switching.page_epoch);
    assert.match((await call('dom')).result.html, /One page/);
    const lastClosed = (await call('tab_close', { tab_id: firstId, expected_epoch: closedActive.page_epoch })).result;
    assert.equal(lastClosed.tabs.length, 1);
    assert.notEqual(lastClosed.active_tab_id, firstId);
    assert.equal(lastClosed.url, 'about:blank');
    let bounded = lastClosed;
    for (let count = 1; count < 8; count++) {
      bounded = (await call('tab_create', { expected_epoch: bounded.page_epoch })).result;
    }
    assert.equal(bounded.tabs.length, 8);
    assert.equal((await call('tab_create', { expected_epoch: bounded.page_epoch })).code, 'invalid_request');
    assert.equal((await call('close')).result.closed, true);
    if (host.exitCode === null) {
      await new Promise((resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error('browser host did not exit after close')), 5000);
        host.once('exit', () => { clearTimeout(timeout); resolve(); });
      });
    }
  } finally {
    host.stdin.end();
    host.kill();
    fixture.close();
    await once(fixture, 'close');
  }
});

test('hover and straight drag change the shared page and reject stale coordinates', async () => {
  const fixture = http.createServer((request, response) => {
    response.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
    if (request.url === '/after-drop') {
      response.end('<main>After drop navigation</main>');
      return;
    }
    if (request.url === '/after-down') {
      response.end('<button id="probe" onclick="document.querySelector(\'output\').textContent=\'clicked\'">Probe</button><output>idle</output>');
      return;
    }
    if (request.url === '/after-move') {
      response.end('<main>After move navigation</main>');
      return;
    }
    if (['/navigate-on-drop', '/navigate-on-down', '/navigate-on-move'].includes(request.url)) {
      const onDown = request.url === '/navigate-on-down' ? 'onmousedown="location.href=\'/after-down\'"' : '';
      const onMove = request.url === '/navigate-on-move' ? 'onpointermove="if(event.buttons)location.href=\'/after-move\'"' : '';
      const onDrop = request.url === '/navigate-on-drop' ? 'location.href=\'/after-drop\'' : '';
      response.end(`<!doctype html><style>
        body { margin: 0; }
        #source { position: absolute; left: 20px; top: 80px; width: 80px; height: 80px; background: blue; }
        #drop { position: absolute; left: 220px; top: 80px; width: 100px; height: 80px; background: green; }
      </style><div id="source" draggable="true" ${onDown} ${onMove} ondragstart="event.dataTransfer.setData('text/plain','source')">Drag</div>
      <div id="drop" ondragover="event.preventDefault()" ondrop="event.preventDefault();${onDrop}">Drop</div>`);
      return;
    }
    response.end(`<!doctype html><style>
      body { margin: 0; }
      #hover { position: absolute; left: 20px; top: 20px; width: 80px; height: 30px; }
      #source { position: absolute; left: 20px; top: 80px; width: 80px; height: 80px; background: blue; }
      #drop { position: absolute; left: 220px; top: 80px; width: 100px; height: 80px; background: green; }
    </style>
    <button id="hover" onpointerenter="const out=document.querySelector('#hovered');out.textContent=String(Number(out.textContent)+1)">Hover</button>
    <div id="source" draggable="true" ondragstart="event.dataTransfer.setData('text/plain','source')">Drag</div>
    <div id="drop" ondragover="event.preventDefault()" ondrop="event.preventDefault(); document.querySelector('#dropped').textContent=event.dataTransfer.getData('text/plain')+'-'+(++window.dropCount)">Drop</div>
    <output id="hovered">0</output><output id="dropped">idle</output><script>window.dropCount=0</script>`);
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
  const lines = readline.createInterface({ input: host.stdout });
  lines.on('line', line => {
    const message = JSON.parse(line);
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
    const initial = (await call('state')).result;
    const navigated = await call('navigate', { url, expected_epoch: initial.page_epoch });
    assert.equal(navigated.ok, true);
    const epoch = navigated.result.page_epoch;
    assert.equal((await call('hover_selector', { selector: '#hover', expected_epoch: epoch })).ok, true);
    assert.match((await call('dom')).result.html, /<output id="hovered">1<\/output>/);
    assert.equal((await call('hover_at', { x: 150, y: 35, expected_epoch: epoch })).ok, true);
    assert.equal((await call('hover_at', { x: 60, y: 35, expected_epoch: epoch })).ok, true);
    assert.match((await call('dom')).result.html, /<output id="hovered">2<\/output>/);
    assert.equal((await call('drag_selector', {
      source_selector: '#source', target_selector: '#drop', expected_epoch: epoch,
    })).ok, true);
    assert.match((await call('dom')).result.html, /<output id="dropped">source-1<\/output>/);
    assert.equal((await call('drag_at', {
      x: 60, y: 120, to_x: 270, to_y: 120, expected_epoch: epoch,
    })).ok, true);
    assert.match((await call('dom')).result.html, /<output id="dropped">source-2<\/output>/);
    const image = await call('screenshot');
    assert.equal(image.ok, true);
    assert.equal(image.result.page_epoch, epoch);
    assert.equal(image.result.active_tab_id, navigated.result.active_tab_id);
    assert.ok(Buffer.from(image.result.data, 'base64').length > 1000);
    assert.equal((await call('hover_at', { x: 1500, y: 20, expected_epoch: epoch })).code, 'invalid_request');
    const resized = await call('viewport', { width: 640, height: 480, expected_epoch: epoch });
    assert.equal(resized.ok, true);
    assert.equal((await call('drag_at', {
      x: 60, y: 120, to_x: 270, to_y: 120, expected_epoch: epoch,
    })).code, 'stale_epoch');
    assert.equal((await call('hover_selector', { selector: '#hover', expected_epoch: epoch })).code, 'stale_epoch');

    const dropReady = await call('navigate', { url: url + 'navigate-on-drop', expected_epoch: resized.result.page_epoch });
    assert.equal(dropReady.ok, true);
    const dropNav = await call('drag_selector', {
      source_selector: '#source', target_selector: '#drop', expected_epoch: dropReady.result.page_epoch,
    });
    assert.equal(dropNav.ok, true);
    assert.match(dropNav.result.url, /\/after-drop$/);
    assert.notEqual(dropNav.result.page_epoch, dropReady.result.page_epoch);
    assert.match((await call('dom')).result.html, /After drop navigation/);
    assert.equal((await call('drag_at', {
      x: 60, y: 120, to_x: 270, to_y: 120, expected_epoch: dropReady.result.page_epoch,
    })).code, 'stale_epoch');

    const downReady = await call('navigate', { url: url + 'navigate-on-down', expected_epoch: dropNav.result.page_epoch });
    assert.equal(downReady.ok, true);
    const downNav = await call('drag_at', {
      x: 60, y: 120, to_x: 270, to_y: 120, expected_epoch: downReady.result.page_epoch,
    });
    assert.equal(downNav.ok, true);
    assert.match(downNav.result.url, /\/after-down$/);
    assert.notEqual(downNav.result.page_epoch, downReady.result.page_epoch);
    assert.equal((await call('click_selector', { selector: '#probe', expected_epoch: downNav.result.page_epoch })).ok, true);
    assert.match((await call('dom')).result.html, /<output>clicked<\/output>/);

    const moveReady = await call('navigate', { url: url + 'navigate-on-move', expected_epoch: downNav.result.page_epoch });
    assert.equal(moveReady.ok, true);
    const moveNav = await call('drag_at', {
      x: 60, y: 120, to_x: 270, to_y: 120, expected_epoch: moveReady.result.page_epoch,
    });
    assert.equal(moveNav.ok, true);
    assert.match(moveNav.result.url, /\/after-move$/);
    assert.notEqual(moveNav.result.page_epoch, moveReady.result.page_epoch);
    assert.match((await call('dom')).result.html, /After move navigation/);
  } finally {
    host.stdin.end();
    host.kill();
    fixture.close();
    await once(fixture, 'close');
  }
});
