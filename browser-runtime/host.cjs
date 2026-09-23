#!/usr/bin/env node
// One host process owns one isolated Chromium context and a bounded tab set. Bamboo is the
// only client: newline-delimited JSON over stdio never exposes a CDP port.
const { chromium } = require('playwright-core');
const { randomBytes } = require('node:crypto');
const readline = require('node:readline');

const MAX_SNAPSHOT_CHARS = 80_000;
const MAX_HTML_CHARS = 100_000;
const MAX_TABS = 8;
let epoch = randomBytes(6).readUIntBE(0, 6);
let browser;
let context;
let tabs = [];
const tabByPage = new WeakMap();
let activeTabId;
let activeCapture;
let captureGeneration = 0;
let captureTask = Promise.resolve();
let frameSeq = 0;
let lastFrameAt = 0;
let closing = false;
let shuttingDown = false;
let browserClosed = false;

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

function activeTab() {
  return tabs.find(tab => tab.id === activeTabId);
}

function requireActiveTab() {
  const tab = activeTab();
  if (!tab || tab.page.isClosed()) throw staleEpochError();
  return tab;
}

function tabSummary(tab) {
  return {
    tab_id: tab.id,
    url: tab.page.url(),
    title: tab.title,
    active: tab.id === activeTabId,
  };
}

async function stableRead(read) {
  for (let attempt = 0; attempt < 3; attempt++) {
    const tab = requireActiveTab();
    const readEpoch = epoch;
    const readUrl = tab.page.url();
    checkPageUrl(readUrl);
    try {
      const value = await read(tab);
      if (epoch === readEpoch && activeTabId === tab.id &&
          !tab.page.isClosed() && tab.page.url() === readUrl) {
        return { page_epoch: readEpoch, active_tab_id: tab.id, url: readUrl, ...value };
      }
    } catch (error) {
      if (epoch === readEpoch && activeTabId === tab.id &&
          !tab.page.isClosed() && tab.page.url() === readUrl) throw error;
    }
  }
  throw staleEpochError();
}

async function state() {
  return stableRead(async tab => {
    const viewport = tab.page.viewportSize();
    const title = await tab.page.title();
    tab.title = title;
    let canGoBack = false;
    let canGoForward = false;
    try {
      const cdp = await tab.cdp;
      const history = await cdp.send('Page.getNavigationHistory');
      canGoBack = history.currentIndex > 0;
      canGoForward = history.currentIndex < history.entries.length - 1;
    } catch {
      // Navigation history is a hint and may be briefly unavailable.
    }
    return {
      frame_seq: frameSeq, title, viewport,
      can_go_back: canGoBack, can_go_forward: canGoForward,
      tabs: tabs.map(tabSummary),
    };
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

function checkPageUrl(raw) {
  return raw === 'about:blank' ? raw : checkUrl(raw);
}

function requireTabId(raw) {
  if (typeof raw !== 'string' || !/^[0-9a-f]{24}$/.test(raw)) {
    const error = new Error('invalid browser tab ID');
    error.code = 'invalid_request';
    throw error;
  }
  return raw;
}

function scheduleCapture() {
  const generation = ++captureGeneration;
  captureTask = captureTask.catch(() => {}).then(async () => {
    if (generation !== captureGeneration || shuttingDown || closing) return;
    if (activeCapture) {
      const previous = activeCapture;
      activeCapture = undefined;
      await previous.page.screencast.stop().catch(() => {});
    }
    if (generation !== captureGeneration || shuttingDown || closing) return;
    const tab = activeTab();
    if (!tab || tab.page.isClosed()) return;
    const capture = { page: tab.page, tabId: tab.id, generation };
    activeCapture = capture;
    try {
      await tab.page.screencast.start({
        quality: 75,
        size: { width: 1200, height: 1000 },
        onFrame: ({ data, viewportWidth, viewportHeight }) => {
          if (captureGeneration !== generation || activeTabId !== tab.id ||
              activeCapture !== capture || tab.page.isClosed() || shuttingDown || closing) return;
          const now = Date.now();
          if (now - lastFrameAt < 100) return;
          lastFrameAt = now;
          emit({
            event: 'frame', active_tab_id: tab.id, page_epoch: epoch,
            frame_seq: ++frameSeq, viewport_width: viewportWidth,
            viewport_height: viewportHeight, data: data.toString('base64'),
          });
        },
      });
    } catch {
      if (activeCapture === capture) activeCapture = undefined;
    }
  });
}

function advanceEpoch() {
  epoch++;
  lastFrameAt = 0;
  // Reset the server's cached JPEG before replying with the new active state.
  emit({ event: 'frame_reset', active_tab_id: activeTabId || null, page_epoch: epoch });
  // Restarting screencast yields a first current-epoch JPEG even if a hidden
  // iframe changes without repainting the visible page.
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

function selectOptionArgs(args) {
  if (typeof args.selector !== 'string' || !args.selector.trim() ||
      args.selector.length > 512) {
    throw targetError('invalid_target', 'browser select requires a bounded CSS selector');
  }
  if (!Array.isArray(args.values) || args.values.length < 1 || args.values.length > 16 ||
      args.values.some(value => typeof value !== 'string' || Buffer.byteLength(value, 'utf8') > 512)) {
    throw targetError('invalid_target', 'browser select requires 1..16 option values of at most 512 bytes each');
  }
  return args.values;
}

async function waitForTarget(locator, missingMessage) {
  try {
    await locator.first().waitFor({ state: 'attached', timeout: 10_000 });
  } catch (error) {
    if (error.name !== 'TimeoutError') throw error;
    throw targetError('target_not_found', missingMessage);
  }
}

async function targetLocator(args, page) {
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
  const page = requireActiveTab().page;
  const locator = await targetLocator(args, page);
  checkEpoch(args);
  // A Locator may re-resolve after navigation while waiting for an old
  // disabled element. An ElementHandle stays bound to its document.
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

function pointerPoint(args, xName, yName, page) {
  const viewport = page.viewportSize();
  const x = args[xName];
  const y = args[yName];
  if (!Number.isFinite(x) || !Number.isFinite(y) ||
      x < 0 || y < 0 || x >= viewport.width || y >= viewport.height) {
    throw targetError('invalid_request', 'browser pointer coordinate is outside the viewport');
  }
  return { x, y };
}

function pointerSelector(value) {
  if (typeof value !== 'string' || !value.trim() || value.length > 512) {
    throw targetError('invalid_request', 'invalid browser pointer selector');
  }
  return value;
}

function pointerButton(value) {
  const button = value ?? 'left';
  if (!['left', 'right', 'middle'].includes(button)) {
    throw targetError('invalid_request', 'invalid browser pointer button');
  }
  return button;
}

async function observeActionNavigation(page) {
  let started = false;
  let committed = false;
  let failed = false;
  let currentRequest = null;
  let revision = 0;
  let observedPage = page;
  let blankPopupExpected = false;
  const listeners = [];
  const cdp = await tabByPage.get(page)?.cdp;
  if (!cdp) throw targetError('browser_error', 'browser page navigation observer unavailable');
  await cdp.send('Page.enable');
  const onWindowOpen = event => {
    // Playwright's popup/page events can be delayed until a slow destination
    // response starts. CDP reports window.open at the triggering gesture.
    started = true;
    committed = false;
    failed = false;
    currentRequest = null;
    blankPopupExpected = !event.url || event.url === 'about:blank';
    revision++;
  };
  cdp.on('Page.windowOpen', onWindowOpen);
  const mainRequest = (target, request) => {
    if (!request.isNavigationRequest()) return false;
    try { return request.frame() === target.mainFrame(); } catch { return false; }
  };
  const watch = (target, popup = false) => {
    if (listeners.some(item => item.page === target)) return;
    if (popup) {
      // adoptPage activates popups before their navigation commits. An initial
      // about:blank page is not the destination of window.open(url).
      observedPage = target;
      started = true;
      committed = target.url() !== 'about:blank' || blankPopupExpected;
      failed = false;
      currentRequest = null;
      revision++;
    }
    const onRequest = request => {
      if (target !== observedPage || !mainRequest(target, request)) return;
      started = true;
      committed = false;
      failed = false;
      currentRequest = request;
      revision++;
    };
    const onFailed = request => {
      if (target !== observedPage || request !== currentRequest) return;
      failed = true;
      currentRequest = null;
      revision++;
    };
    const onFrame = frame => {
      if (target !== observedPage || frame !== target.mainFrame()) return;
      if (target !== page && frame.url() === 'about:blank' && !blankPopupExpected) return;
      started = true;
      committed = true;
      currentRequest = null;
      revision++;
    };
    const onClose = () => {
      if (target !== observedPage) return;
      failed = true;
      revision++;
    };
    target.on('request', onRequest);
    target.on('requestfailed', onFailed);
    target.on('framenavigated', onFrame);
    target.on('close', onClose);
    listeners.push({ page: target, onRequest, onFailed, onFrame, onClose });
  };
  const onPopup = target => { watch(target, true); };
  watch(page);
  page.on('popup', onPopup);
  return {
    get started() { return started; },
    async finish() {
      // A newly committed document can immediately request another navigation.
      // Each request invalidates the prior commit; return only after the latest
      // request commits and navigation events have settled for one short turn.
      const deadline = Date.now() + 20_000;
      let observedRevision = revision;
      while (Date.now() < deadline) {
        await new Promise(resolve => setTimeout(resolve, 50));
        if (observedRevision !== revision) {
          observedRevision = revision;
          continue;
        }
        if (failed) break;
        if (!started || committed) return;
      }
      if (!failed && started && !committed) {
        throw targetError('navigation_timeout', 'browser navigation did not complete');
      }
      if (failed) throw targetError('navigation_failed', 'browser navigation failed');
    },
    dispose() {
      cdp.off('Page.windowOpen', onWindowOpen);
      page.off('popup', onPopup);
      for (const { page: target, onRequest, onFailed, onFrame, onClose } of listeners) {
        target.off('request', onRequest);
        target.off('requestfailed', onFailed);
        target.off('framenavigated', onFrame);
        target.off('close', onClose);
      }
    },
  };
}

async function hoverWithNavigation(page, expectedEpoch, hover) {
  const navigation = await observeActionNavigation(page);
  try {
    try { await hover(); } catch (error) {
      if (!navigation.started && expectedEpoch === epoch) throw error;
    }
    await navigation.finish();
  } finally {
    navigation.dispose();
  }
}

async function dragBetween(page, source, destination, expectedEpoch, button = 'left') {
  if (expectedEpoch !== epoch) throw staleEpochError();
  const navigation = await observeActionNavigation(page);
  const interrupted = () => expectedEpoch !== epoch || navigation.started;
  let downAttempted = false;
  let failure;
  try {
    try {
      await page.mouse.move(source.x, source.y);
      if (!interrupted()) {
        downAttempted = true;
        await page.mouse.down({ button });
        let current = source;
        if (!interrupted() && typeof destination === 'function') {
          const viewport = page.viewportSize();
          current = {
            x: source.x + (source.x + 8 < viewport.width ? 8 : -8),
            y: source.y + (source.y + 8 < viewport.height ? 8 : -8),
          };
          await page.mouse.move(current.x, current.y);
          if (!interrupted()) destination = await destination();
        }
        if (!interrupted()) {
          for (let step = 1; step <= 12; step++) {
            if (interrupted()) break;
            await page.mouse.move(
              current.x + (destination.x - current.x) * step / 12,
              current.y + (destination.y - current.y) * step / 12,
            );
          }
        }
      }
    } catch (error) {
      if (!interrupted()) failure = error;
    } finally {
      if (downAttempted) {
        // Clear button state without finishing the old gesture on a new page.
        if (interrupted()) await page.mouse.move(-1, -1).catch(() => {});
        await page.mouse.up({ button }).catch(() => {});
      }
    }
    await navigation.finish();
    if (failure) throw failure;
  } finally {
    navigation.dispose();
  }
}

function activateTab(tab) {
  if (activeTabId === tab.id) return;
  activeTabId = tab.id;
  advanceEpoch();
}

function adoptPage(target) {
  if (shuttingDown) {
    void target.close().catch(() => {});
    return null;
  }
  const existing = tabByPage.get(target);
  if (existing) return existing;
  if (tabs.length >= MAX_TABS) {
    void target.close().catch(() => {});
    return null;
  }
  const tab = {
    id: randomBytes(12).toString('hex'),
    page: target,
    cdp: context.newCDPSession(target).catch(() => null),
    title: '',
  };
  tabs.push(tab);
  tabByPage.set(target, tab);
  target.setDefaultTimeout(10_000);
  target.on('domcontentloaded', () => {
    void target.title().then(title => { tab.title = title; }).catch(() => {});
  });
  target.on('framenavigated', frame => {
    if (frame === target.mainFrame()) {
      const url = frame.url();
      if (url !== 'about:blank') {
        try { checkUrl(url); } catch { void target.goto('about:blank').catch(() => {}); }
      }
    }
    // Every frame navigation invalidates coordinates and semantic targets in
    // the active view, including iframe content.
    if (activeTabId === tab.id) advanceEpoch();
  });
  target.on('close', () => {
    tabs = tabs.filter(candidate => candidate !== tab);
    if (activeTabId !== tab.id) return;
    activeTabId = tabs.at(-1)?.id;
    if (!shuttingDown) {
      advanceEpoch();
      if (!activeTabId) void context.newPage().catch(() => {});
    }
  });
  // A popup becomes the visible workbench tab. Explicit new tabs use this same
  // path, so a page cannot exist without an opaque ID and navigation guard.
  activateTab(tab);
  return tab;
}

async function command(action, args = {}) {
  switch (action) {
    case 'state':
    case 'tab_list':
      return state();
    case 'tab_create':
      checkEpoch(args);
      if (tabs.length >= MAX_TABS) {
        const error = new Error('browser tab limit reached');
        error.code = 'invalid_request';
        throw error;
      }
      adoptPage(await context.newPage());
      return state();
    case 'tab_activate': {
      checkEpoch(args);
      const tabId = requireTabId(args.tab_id);
      const tab = tabs.find(tab => tab.id === tabId);
      if (!tab) {
        const error = new Error('browser tab not found');
        error.code = 'invalid_request';
        throw error;
      }
      activateTab(tab);
      return state();
    }
    case 'tab_close': {
      checkEpoch(args);
      const tabId = requireTabId(args.tab_id);
      const tab = tabs.find(tab => tab.id === tabId);
      if (!tab) {
        const error = new Error('browser tab not found');
        error.code = 'invalid_request';
        throw error;
      }
      // Keep a valid active view when the last tab is closed.
      if (tabs.length === 1) adoptPage(await context.newPage());
      await tab.page.close();
      return state();
    }
    case 'navigate':
      checkEpoch(args);
      var page = requireActiveTab().page;
      await page.goto(checkUrl(args.url), { waitUntil: 'domcontentloaded', timeout: 20_000 });
      return state();
    case 'history':
      checkEpoch(args);
      page = requireActiveTab().page;
      if (args.direction === 'back') await page.goBack({ waitUntil: 'domcontentloaded', timeout: 20_000 });
      else if (args.direction === 'forward') await page.goForward({ waitUntil: 'domcontentloaded', timeout: 20_000 });
      else if (args.direction === 'reload') await page.reload({ waitUntil: 'domcontentloaded', timeout: 20_000 });
      else throw new Error('invalid history direction');
      return state();
    case 'viewport':
      checkEpoch(args);
      page = requireActiveTab().page;
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
      page = requireActiveTab().page;
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
      return stableRead(async tab => {
        const page = tab.page;
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
      } else {
        await requireActiveTab().page.keyboard.press(args.key);
      }
      return state();
    case 'select_option': {
      checkEpoch(args);
      const values = selectOptionArgs(args);
      try {
        const selectedValues = await withPinnedTarget(args, async handle => {
          const identity = await handle.evaluate(element => ({
            tag_name: element.tagName,
            multiple: element.multiple,
          }));
          if (identity.tag_name !== 'SELECT') {
            throw targetError('invalid_target', 'browser select target must be a native select element');
          }
          if (!identity.multiple && values.length > 1) {
            throw targetError('invalid_target', 'single-select target accepts exactly one value');
          }
          return handle.selectOption(values, { timeout: 10_000 });
        });
        return { ...await state(), selected_values: selectedValues };
      } catch (error) {
        if (['invalid_target', 'target_not_found', 'ambiguous_target', 'stale_epoch'].includes(error?.code)) {
          throw error;
        }
        // Playwright's call log can quote option values. Keep the failure
        // actionable without copying page data into the tool error.
        throw targetError('selection_failed', 'browser select option failed; refresh the page and retry');
      }
    }
    case 'hover_selector':
      checkEpoch(args);
      page = requireActiveTab().page;
      await hoverWithNavigation(page, args.expected_epoch,
        () => withPinnedTarget(args, handle => handle.hover({ timeout: 10_000 })));
      return state();
    case 'hover_at':
      checkEpoch(args);
      page = requireActiveTab().page;
      var hoverPoint = pointerPoint(args, 'x', 'y', page);
      checkEpoch(args);
      await hoverWithNavigation(page, args.expected_epoch,
        () => page.mouse.move(hoverPoint.x, hoverPoint.y));
      return state();
    case 'drag_selector':
      checkEpoch(args);
      page = requireActiveTab().page;
      var sourceSelector = pointerSelector(args.source_selector);
      var targetSelector = pointerSelector(args.target_selector);
      await withPinnedTarget({ selector: sourceSelector, expected_epoch: args.expected_epoch }, async source => {
        await withPinnedTarget({ selector: targetSelector, expected_epoch: args.expected_epoch }, async destination => {
          await source.scrollIntoViewIfNeeded({ timeout: 10_000 });
          checkEpoch(args);
          const from = await source.boundingBox();
          const to = await destination.boundingBox();
          if (!from || !to) throw targetError('target_not_found', 'browser drag target is detached');
          const start = pointerPoint({ x: from.x + from.width / 2, y: from.y + from.height / 2 }, 'x', 'y', page);
          const viewport = page.viewportSize();
          const targetX = to.x + to.width / 2;
          const targetY = to.y + to.height / 2;
          const targetVisible = targetX >= 0 && targetY >= 0 &&
            targetX < viewport.width && targetY < viewport.height;
          const end = targetVisible
            ? pointerPoint({ x: targetX, y: targetY }, 'x', 'y', page)
            : async () => {
              // Begin the drag on the visible source before scrolling a distant
              // destination into view; both elements need not fit together.
              await destination.scrollIntoViewIfNeeded({ timeout: 10_000 });
              const box = await destination.boundingBox();
              if (!box) throw targetError('target_not_found', 'browser drag target is detached');
              return pointerPoint({ x: box.x + box.width / 2, y: box.y + box.height / 2 }, 'x', 'y', page);
            };
          checkEpoch(args);
          await dragBetween(page, start, end, args.expected_epoch);
        });
      });
      return state();
    case 'drag_at':
      checkEpoch(args);
      page = requireActiveTab().page;
      var from = pointerPoint(args, 'x', 'y', page);
      var to = pointerPoint(args, 'to_x', 'to_y', page);
      var button = pointerButton(args.button);
      checkEpoch(args);
      await dragBetween(page, from, to, args.expected_epoch, button);
      return state();
    case 'screenshot': {
      return stableRead(async tab => {
        const page = tab.page;
        const viewport = page.viewportSize();
        const data = await page.screenshot({ type: 'jpeg', quality: 80, scale: 'css', timeout: 10_000 });
        return { viewport, mime_type: 'image/jpeg', data: data.toString('base64') };
      });
    }
    case 'close':
      shuttingDown = true;
      await browser.close();
      browserClosed = true;
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
  await context.route('**/*', route => {
    const request = route.request();
    if (request.isNavigationRequest()) {
      try {
        checkUrl(request.url());
      } catch {
        return route.abort('blockedbyclient');
      }
    }
    return route.continue();
  });
  context.on('page', target => { adoptPage(target); });
  adoptPage(await context.newPage());
  await captureTask;

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
  shuttingDown = true;
  lines.close();
  process.stdin.pause();
  if (!browserClosed) await browser.close().catch(() => {});
}

main().catch(error => {
  process.stderr.write(`browser host failed: ${error.message || error}\n`);
  process.exitCode = 1;
});
