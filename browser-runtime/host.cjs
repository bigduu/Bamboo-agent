#!/usr/bin/env node
// One host process owns one isolated Chromium context and page. Bamboo is the
// only client: newline-delimited JSON over stdio never exposes a CDP port.
const { chromium } = require('playwright-core');
const { randomBytes } = require('node:crypto');
const readline = require('node:readline');

const MAX_SNAPSHOT_CHARS = 80_000;
const MAX_HTML_CHARS = 100_000;
let epoch = randomBytes(6).readUIntBE(0, 6);
let browser;
let context;
let page;
let cdp;
let frameSeq = 0;
let lastFrameAt = 0;
let captureGeneration = 0;
let captureTask = Promise.resolve();
let closing = false;

function emit(message) {
  if (!closing) process.stdout.write(`${JSON.stringify(message)}\n`);
}

function bounded(value, maximum) {
  return value.length > maximum ? value.slice(0, maximum) : value;
}

function staleEpochError() {
  const error = new Error('browser page changed; refresh state and retry');
  error.code = 'stale_epoch';
  return error;
}

async function stableRead(read) {
  for (let attempt = 0; attempt < 3; attempt++) {
    const readEpoch = epoch;
    const readUrl = page.url();
    try {
      const value = await read();
      if (epoch === readEpoch && page.url() === readUrl) {
        return { page_epoch: readEpoch, url: readUrl, ...value };
      }
    } catch (error) {
      if (epoch === readEpoch && page.url() === readUrl) throw error;
    }
  }
  throw staleEpochError();
}

async function state() {
  return stableRead(async () => {
    const viewport = page.viewportSize();
    const title = await page.title();
    let canGoBack = false;
    let canGoForward = false;
    try {
      const history = await cdp.send('Page.getNavigationHistory');
      canGoBack = history.currentIndex > 0;
      canGoForward = history.currentIndex < history.entries.length - 1;
    } catch {
      // Navigation history is a hint and may be briefly unavailable.
    }
    return { frame_seq: frameSeq, title, viewport, can_go_back: canGoBack, can_go_forward: canGoForward };
  });
}

function checkEpoch(args) {
  if (args.expected_epoch !== epoch) {
    throw staleEpochError();
  }
}

function checkUrl(raw) {
  const value = new URL(raw);
  if (value.protocol !== 'http:' && value.protocol !== 'https:') {
    const error = new Error('only http and https pages are supported');
    error.code = 'invalid_url';
    throw error;
  }
  if (value.username || value.password) {
    const error = new Error('URLs with embedded credentials are not supported');
    error.code = 'invalid_url';
    throw error;
  }
  return value.href;
}

function scheduleCapture() {
  const generation = ++captureGeneration;
  captureTask = captureTask.catch(() => {}).then(async () => {
    if (generation !== captureGeneration || closing || page.isClosed()) return;
    await page.screencast.stop().catch(() => {});
    if (generation !== captureGeneration || closing || page.isClosed()) return;
    await page.screencast.start({
      quality: 75,
      size: { width: 1200, height: 1000 },
      onFrame: ({ data, viewportWidth, viewportHeight }) => {
        if (generation !== captureGeneration || closing || page.isClosed()) return;
        const now = Date.now();
        if (now - lastFrameAt < 100) return;
        lastFrameAt = now;
        emit({ event: 'frame', page_epoch: epoch, frame_seq: ++frameSeq,
          viewport_width: viewportWidth, viewport_height: viewportHeight,
          data: data.toString('base64') });
      },
    });
  });
  return captureTask;
}

function advanceEpoch() {
  epoch++;
  lastFrameAt = 0;
  // Drop an old JPEG before returning a state for the new frame document.
  emit({ event: 'frame_reset', page_epoch: epoch });
  // Restarting screencast produces a current-epoch first frame even when a
  // hidden iframe changes without repainting the visible page.
  void scheduleCapture();
}

function targetError(code, message) {
  const error = new Error(message);
  error.code = code;
  return error;
}

function semanticString(target, name, maximum, required = false) {
  const value = target[name];
  if (value === undefined && !required) return undefined;
  if (typeof value !== 'string' || !value.trim() || value.length > maximum) {
    throw targetError('invalid_target', `invalid browser target ${name}`);
  }
  return value;
}

async function waitForTarget(locator, missingMessage) {
  try {
    await locator.first().waitFor({ state: 'attached', timeout: 10_000 });
  } catch (error) {
    if (error.name !== 'TimeoutError') throw error;
    throw targetError('target_not_found', missingMessage);
  }
}

async function targetLocator(args) {
  let locator;
  if (args.target === undefined) {
    if (typeof args.selector !== 'string' || !args.selector.trim()) {
      throw targetError('invalid_target', 'browser action requires a selector or target');
    }
    locator = page.locator(args.selector);
  } else {
    if (args.selector !== undefined && args.selector !== null) {
      throw targetError('invalid_target', 'selector and target are mutually exclusive');
    }
    const target = args.target;
    if (!target || typeof target !== 'object' || Array.isArray(target)) {
      throw targetError('invalid_target', 'invalid browser semantic target');
    }
    const kind = target.kind;
    const allowed = kind === 'role'
      ? ['kind', 'role', 'name', 'exact', 'frame_selector']
      : kind === 'label' || kind === 'text'
        ? ['kind', 'value', 'exact', 'frame_selector']
        : null;
    if (!allowed || Object.keys(target).some(key => !allowed.includes(key)) ||
        (target.exact !== undefined && typeof target.exact !== 'boolean')) {
      throw targetError('invalid_target', 'invalid browser semantic target');
    }
    const frameSelector = semanticString(target, 'frame_selector', 512);
    let scope = page;
    if (frameSelector) {
      const owner = page.frameLocator(frameSelector).owner();
      await waitForTarget(owner, 'browser target iframe not found');
      const count = await owner.count();
      if (count === 0) throw targetError('target_not_found', 'browser target iframe not found');
      if (count !== 1) {
        throw targetError('ambiguous_target', `browser target iframe matched ${count} elements`);
      }
      scope = page.frameLocator(frameSelector);
    }
    const exact = target.exact ?? true;
    if (kind === 'role') {
      const role = semanticString(target, 'role', 64, true);
      if (!/^[a-z-]+$/.test(role)) {
        throw targetError('invalid_target', 'invalid browser target role');
      }
      const name = semanticString(target, 'name', 256);
      locator = scope.getByRole(role, name === undefined ? {} : { name, exact });
    } else {
      const value = semanticString(target, 'value', 256, true);
      locator = kind === 'label'
        ? scope.getByLabel(value, { exact })
        : scope.getByText(value, { exact });
    }
  }
  await waitForTarget(locator, 'browser target not found');
  const count = await locator.count();
  if (count === 0) throw targetError('target_not_found', 'browser target not found');
  if (count !== 1) {
    throw targetError('ambiguous_target', `browser target matched ${count} elements`);
  }
  return locator;
}

async function withPinnedTarget(args, act) {
  const locator = await targetLocator(args);
  checkEpoch(args);
  // Locator actions can silently re-resolve on a new document while waiting
  // for an old disabled element. A handle is bound to the resolved document;
  // navigation detaches it instead of retargeting the action.
  const handle = await locator.elementHandle({ timeout: 10_000 });
  if (!handle) throw targetError('target_not_found', 'browser target not found');
  try {
    const count = await locator.count();
    if (count !== 1) {
      throw targetError(
        count === 0 ? 'target_not_found' : 'ambiguous_target',
        `browser target matched ${count} elements`,
      );
    }
    checkEpoch(args);
    return await act(handle);
  } finally {
    await handle.dispose().catch(() => {});
  }
}

async function command(action, args = {}) {
  switch (action) {
    case 'state':
      return state();
    case 'navigate':
      checkEpoch(args);
      await page.goto(checkUrl(args.url), { waitUntil: 'domcontentloaded', timeout: 20_000 });
      return state();
    case 'history':
      checkEpoch(args);
      if (args.direction === 'back') await page.goBack({ waitUntil: 'domcontentloaded', timeout: 20_000 });
      else if (args.direction === 'forward') await page.goForward({ waitUntil: 'domcontentloaded', timeout: 20_000 });
      else if (args.direction === 'reload') await page.reload({ waitUntil: 'domcontentloaded', timeout: 20_000 });
      else throw new Error('invalid history direction');
      return state();
    case 'viewport':
      checkEpoch(args);
      if (!Number.isInteger(args.width) || !Number.isInteger(args.height) ||
          args.width < 320 || args.width > 1200 || args.height < 240 || args.height > 1000) {
        throw new Error('viewport must be within 320..1200 by 240..1000 CSS pixels');
      }
      if (page.viewportSize().width !== args.width || page.viewportSize().height !== args.height) {
        await page.setViewportSize({ width: args.width, height: args.height });
        // Frame coordinates from the prior viewport must not be applied to
        // the resized page, even if its URL has not changed.
        advanceEpoch();
      }
      return state();
    case 'input':
      checkEpoch(args);
      if (args.kind === 'click') {
        await page.mouse.click(args.x, args.y, { button: args.button || 'left' });
      } else if (args.kind === 'scroll') {
        await page.mouse.move(args.x, args.y);
        await page.mouse.wheel(args.delta_x, args.delta_y);
      } else if (args.kind === 'type') {
        await page.keyboard.insertText(args.text);
      } else if (args.kind === 'key') {
        await page.keyboard.press(args.key);
      } else {
        throw new Error('invalid input kind');
      }
      return state();
    case 'dom': {
      return stableRead(async () => {
        const snapshot = await page.ariaSnapshot({ mode: 'ai', depth: 12, timeout: 10_000 });
        const html = await page.content();
        return {
          title: await page.title(),
          snapshot: bounded(snapshot, MAX_SNAPSHOT_CHARS),
          html: bounded(html, MAX_HTML_CHARS),
          snapshot_truncated: snapshot.length > MAX_SNAPSHOT_CHARS,
          html_truncated: html.length > MAX_HTML_CHARS,
        };
      });
    }
    case 'click_selector':
      checkEpoch(args);
      await withPinnedTarget(args, handle => handle.click({ timeout: 10_000 }));
      return state();
    case 'fill_selector':
      checkEpoch(args);
      await withPinnedTarget(args, handle => handle.fill(args.text, { timeout: 10_000 }));
      return state();
    case 'press_selector':
      checkEpoch(args);
      if (args.selector || args.target !== undefined) {
        await withPinnedTarget(args, handle => handle.press(args.key, { timeout: 10_000 }));
      }
      else await page.keyboard.press(args.key);
      return state();
    case 'screenshot': {
      return stableRead(async () => {
        const viewport = page.viewportSize();
        const data = await page.screenshot({ type: 'jpeg', quality: 80, scale: 'css', timeout: 10_000 });
        return { viewport, mime_type: 'image/jpeg', data: data.toString('base64') };
      });
    }
    case 'close':
      await browser.close();
      return { closed: true };
    default:
      throw new Error(`unknown browser action: ${action}`);
  }
}

async function main() {
  browser = await chromium.launch({
    headless: true,
    ...(process.env.BAMBOO_BROWSER_EXECUTABLE ? { executablePath: process.env.BAMBOO_BROWSER_EXECUTABLE } : {}),
  });
  context = await browser.newContext({
    viewport: { width: 1000, height: 720 },
    deviceScaleFactor: 1,
    acceptDownloads: false,
    serviceWorkers: 'block',
  });
  page = await context.newPage();
  await page.route('**/*', route => {
    const request = route.request();
    if (request.isNavigationRequest() && request.frame() === page.mainFrame()) {
      try {
        checkUrl(request.url());
      } catch {
        return route.abort('blockedbyclient');
      }
    }
    return route.continue();
  });
  page.on('framenavigated', frame => {
    // A remembered iframe target belongs to the old frame document, even if
    // the top-level URL is unchanged. Invalidate its epoch on every document
    // navigation and let the next screencast frame use the new generation.
    advanceEpoch();
    if (frame === page.mainFrame()) {
      const url = frame.url();
      if (url !== 'about:blank') {
        try { checkUrl(url); } catch { void page.goto('about:blank').catch(() => {}); }
      }
    }
  });
  context.on('page', target => {
    if (target !== page) void target.close().catch(() => {});
  });
  page.setDefaultTimeout(10_000);
  cdp = await context.newCDPSession(page);
  await scheduleCapture();

  const lines = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
  for await (const line of lines) {
    let request;
    try {
      request = JSON.parse(line);
      const result = await command(request.action, request.args);
      emit({ id: request.id, ok: true, result });
      if (request.action === 'close') break;
    } catch (error) {
      emit({ id: request?.id, ok: false, code: error.code || 'browser_error', error: String(error.message || error) });
    }
  }
  closing = true;
  await browser.close().catch(() => {});
}

main().catch(error => {
  process.stderr.write(`browser host failed: ${error.message || error}\n`);
  process.exitCode = 1;
});
