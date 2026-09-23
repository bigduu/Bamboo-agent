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
        epoch++;
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
      await page.locator(args.selector).click({ timeout: 10_000 });
      return state();
    case 'fill_selector':
      checkEpoch(args);
      await page.locator(args.selector).fill(args.text, { timeout: 10_000 });
      return state();
    case 'press_selector':
      checkEpoch(args);
      if (args.selector) await page.locator(args.selector).press(args.key, { timeout: 10_000 });
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
    if (frame === page.mainFrame()) {
      epoch++;
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
  await page.screencast.start({
    quality: 75,
    size: { width: 1200, height: 1000 },
    onFrame: ({ data, viewportWidth, viewportHeight }) => {
      const now = Date.now();
      if (now - lastFrameAt < 100) return;
      lastFrameAt = now;
      emit({ event: 'frame', page_epoch: epoch, frame_seq: ++frameSeq,
        viewport_width: viewportWidth, viewport_height: viewportHeight,
        data: data.toString('base64') });
    },
  });

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
