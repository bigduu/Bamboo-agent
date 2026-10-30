// One isolated real Host and Chromium. No API response is synthesized.
// The unknown-write case deliberately drops a real response after the Host
// has processed it; this is transport fault injection, not a mocked backend.
import assert from 'node:assert/strict'
import { createHash } from 'node:crypto'
import { closeSync, createReadStream, openSync } from 'node:fs'
import { appendFile, mkdir, mkdtemp, readFile, readdir } from 'node:fs/promises'
import { createRequire } from 'node:module'
import net from 'node:net'
import path from 'node:path'
import { spawn, spawnSync } from 'node:child_process'
import { setTimeout as delay } from 'node:timers/promises'
import { clearOutcome, waitForManagedBinding } from './binding.mjs'
import { runIndependentScenarios } from './scenarios.mjs'

const bamboo = process.env.DIAG_BAMBOO
const lotus = process.env.DIAG_LOTUS
const target = process.env.CARGO_TARGET_DIR
const temp = process.env.RUNNER_TEMP
assert.ok(bamboo && lotus && target && temp)
const require = createRequire(path.join(lotus, 'package.json'))
const { chromium } = require('playwright')
const binary = path.join(target, 'debug/bamboo')
const seed = path.join(target, 'debug/examples/bound_history_diagnostic_seed')
const dist = path.join(lotus, 'dist')
const root = await mkdtemp(path.join(temp, 'bound-history-2396-'))
for (const name of ['data', 'home', 'tmp', 'config', 'cache', 'xdg-data', 'jiandu', 'workspace']) {
  await mkdir(path.join(root, name), { mode: 0o700 })
}
const env = {
  PATH: '/usr/bin:/bin:/usr/sbin:/sbin', LANG: 'en_US.UTF-8',
  HOME: path.join(root, 'home'), TMPDIR: path.join(root, 'tmp'),
  XDG_CONFIG_HOME: path.join(root, 'config'), XDG_CACHE_HOME: path.join(root, 'cache'),
  XDG_DATA_HOME: path.join(root, 'xdg-data'), BAMBOO_DATA_DIR: path.join(root, 'data'),
  BAMBOO_JIANDU_DATA_DIR: path.join(root, 'jiandu'), JIANDU_DATA_DIR: path.join(root, 'jiandu'),
  DIAGNOSTIC_DATA_DIR: path.join(root, 'data'), BAMBOO_OTEL_ENABLED: '0', OTEL_SDK_DISABLED: 'true',
}
const ids = {
  success: '10000000-0000-4000-8000-000000000001',
  unknown: '10000000-0000-4000-8000-000000000002',
  conflict: '10000000-0000-4000-8000-000000000003',
}
const result = {
  status: 'RUNNING', synthetic: true, controller: process.env.DISPATCH_SOURCE,
  bamboo_source: process.env.BAMBOO_SOURCE, bamboo_tree: process.env.BAMBOO_TREE,
  lotus_source: process.env.LOTUS_SOURCE, lotus_tree: process.env.LOTUS_TREE,
  diagnostic_requested_provider_execution: false, backend_responses_mocked: false,
  network_scope: 'browser requests are loopback-allowlisted; Host outbound traffic is not independently instrumented',
  unknown_case: 'real C1 response dropped after Host processed the request',
  checks: [], blocked: [], page_errors: [], host_exit: null,
}
function check(name, value) {
  result.checks.push({ name, passed: Boolean(value) })
  console.log(`${value ? 'PASS' : 'FAIL'} ${name}`)
  assert.ok(value, name)
}
const hash = (bytes) => createHash('sha256').update(bytes).digest('hex')
async function hashFile(file) {
  const digest = createHash('sha256')
  for await (const chunk of createReadStream(file)) digest.update(chunk)
  return digest.digest('hex')
}
const manifest = JSON.parse(await readFile(path.join(dist, 'lotus-next-manifest.json'), 'utf8'))
result.host_sha256 = await hashFile(binary)
result.fixture_sha256 = await hashFile(seed)
result.frontend_manifest_sha256 = hash(await readFile(path.join(dist, 'lotus-next-manifest.json')))
result.frontend_resources_sha256 = manifest.resourcesSha256
check('frontend manifest is bound to the exact clean source', manifest.sourceRevision === result.lotus_source && manifest.sourceDirty === false)
const seeded = spawnSync(seed, [], { cwd: path.join(root, 'workspace'), env, encoding: 'utf8', timeout: 30000 })
console.log(seeded.stdout || '')
if (seeded.stderr) console.error(seeded.stderr)
check('real storage fixture seeded synthetic sessions', seeded.status === 0)
const port = await new Promise((resolve, reject) => {
  const socket = net.createServer()
  socket.once('error', reject)
  socket.listen(0, '127.0.0.1', () => {
    const selected = socket.address().port
    socket.close((error) => error ? reject(error) : resolve(selected))
  })
})
const base = `http://127.0.0.1:${port}`
const hostLog = openSync(path.join(root, 'host.log'), 'w', 0o600)
const host = spawn(binary, ['serve', '--bind', '127.0.0.1', '--port', String(port),
  '--data-dir', path.join(root, 'data'), '--static-dir', dist, '--workers', '1'],
{ cwd: path.join(root, 'workspace'), env, stdio: ['ignore', hostLog, hostLog] })
closeSync(hostLog)
let hostExit = null
host.once('exit', (code, signal) => { hostExit = { code, signal } })
host.once('error', error => { hostExit = { error: error.message } })
result.host_pid = host.pid
let browser

async function request(method, urlPath, body) {
  const response = await fetch(base + urlPath, {
    method, headers: body === undefined ? {} : { 'content-type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(15000),
  })
  const bytes = Buffer.from(await response.arrayBuffer())
  let json
  try { json = JSON.parse(bytes.toString('utf8')) } catch { json = undefined }
  return { status: response.status, json, sha256: hash(bytes), bytes }
}
async function ok(method, urlPath, body) {
  const response = await request(method, urlPath, body)
  assert.ok(response.status >= 200 && response.status < 300, `${method} ${urlPath}: ${response.status} ${JSON.stringify(response.json)}`)
  return response.json
}
function sameTarget(target) {
  return Object.fromEntries(['session_id', 'created_at', 'kind', 'root_session_id', 'parent_session_id',
    'project_id', 'authority_sha256', 'ancestry_sha256'].map(key => [key, target[key]]))
}
function assertEmptyReadback(before, after) {
  assert.deepEqual(sameTarget(after.current_user_target), sameTarget(before.current_user_target))
  assert.deepEqual(after.supervisor, before.supervisor)
  assert.equal(after.history_snapshot_source, 'durable')
  assert.equal(after.is_delta, false)
  assert.equal(after.truncated, false)
  assert.equal(after.bound_token_usage, null)
  assert.ok(Array.isArray(after.messages) && after.messages.length <= 1 && after.messages.every(m => m.role === 'system'))
  assert.equal(after.bound_clear_evidence.full_message_count, after.messages.length)
  assert.equal(after.bound_clear_evidence.execution_active, false)
  assert.equal(after.bound_clear_evidence.has_pending_question, false)
  assert.equal(after.bound_clear_evidence.has_pending_approval, false)
}

try {
  const deadline = Date.now() + 60000
  let healthy = false
  while (Date.now() < deadline && !hostExit) {
    try { healthy = (await request('GET', '/healthz')).status === 200 } catch { /* startup */ }
    if (healthy) break
    await delay(250)
  }
  check('owned Host started and healthz returned 200', healthy && !hostExit)
  const served = await request('GET', '/')
  check('Host serves the exact built frontend index', served.status === 200 && served.sha256 === hash(await readFile(path.join(dist, 'index.html'))))
  const setup = await ok('GET', '/api/v1/bamboo/setup/status')
  if (!setup.is_complete) await ok('POST', '/api/v1/bamboo/setup/complete', {})
  check('isolated setup completed', (await ok('GET', '/api/v1/bamboo/setup/status')).is_complete === true)
  await ok('POST', '/api/v1/supervisor/default', {})
  const directory = await ok('GET', '/api/v1/supervisor/default/sessions?limit=20')
  check('real Host advertises bound history', directory.capabilities?.current_user_bound_history_v1 === true)
  const row = directory.sessions.find(item => item.session_id === ids.conflict)
  assert.ok(row?.current_user_target)
  const conflictPath = `/api/v1/supervisor/default/sessions/${ids.conflict}/history`
  const originalSelection = { supervisor: directory.scope.supervisor, target: row.current_user_target }
  const first = await request('POST', conflictPath, originalSelection)
  check('real bound history succeeds before metadata change', first.status === 200 && first.json?.history_snapshot_source === 'durable')
  await ok('PATCH', `/api/v1/sessions/${ids.conflict}`, { title: 'Synthetic conflict renamed' })
  const stale = await request('POST', conflictPath, originalSelection)
  check('real stale metadata selector returns 409 without history', stale.status === 409 && stale.json?.code === 'target_observation_stale' && !('messages' in stale.json))
  const updatedDirectory = await ok('GET', '/api/v1/supervisor/default/sessions?limit=20')
  const updated = updatedDirectory.sessions.find(item => item.session_id === ids.conflict)?.current_user_target
  check('directory exposes the same identity with a newer metadata version', updated && updated.metadata_version > originalSelection.target.metadata_version &&
    JSON.stringify(sameTarget(updated)) === JSON.stringify(sameTarget(originalSelection.target)))
  const refreshed = await request('POST', conflictPath, { supervisor: updatedDirectory.scope.supervisor, target: updated })
  check('real bound history succeeds with refreshed observation', refreshed.status === 200)

  const assets = (await readdir(path.join(dist, 'assets'))).filter(name => /^appStore-.*\.js$/.test(name))
  assert.equal(assets.length, 1)
  const storeAsset = `/assets/${assets[0]}`
  browser = await chromium.launch({ headless: true })

  async function scenario(label, dropResponse) {
    const id = ids[label], marker = `[bound-clear-fixture:${label}]`
    const historyPath = `/api/v1/supervisor/default/sessions/${id}/history`
    const clearPath = `/api/v1/sessions/${id}/clear`
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 }, colorScheme: 'dark',
      locale: 'en-US', serviceWorkers: 'block' })
    await context.addInitScript(() => { localStorage.setItem('bodhi_onboarded_v1', '1'); localStorage.setItem('lotus_ui_locale_v1', 'en-US') })
    await context.routeWebSocket('**/*', async socket => {
      const url = new URL(socket.url())
      if (url.protocol === 'ws:' && url.hostname === '127.0.0.1' && url.port === String(port)) return socket.connectToServer()
      result.blocked.push({ scenario: label, method: 'WEBSOCKET', origin: url.origin })
      return socket.close({ code: 1008, reason: 'diagnostic loopback only' })
    })
    const page = await context.newPage()
    page.on('pageerror', error => result.page_errors.push({ scenario: label, error: error.message }))
    let clearCount = 0, dropped, changeBeforeBoundRead = false, racePatch
    await page.route('**/*', async route => {
      const req = route.request(), url = new URL(req.url()), method = req.method()
      if (url.origin !== base) {
        result.blocked.push({ scenario: label, method, origin: url.origin, path: url.pathname })
        return route.abort()
      }
      if (method === 'POST' && url.pathname === clearPath) {
        clearCount += 1
        if (dropResponse) {
          // Forward the original request to the actual Host exactly once, then
          // drop its response so the UI must report an unknown write outcome.
          try {
            const actual = await route.fetch({ maxRetries: 0 })
            const receipt = await actual.json()
            dropped = { status: actual.status(), success: receipt.success, session_id: receipt.session_id }
          } catch (error) { dropped = { error: String(error) } }
          return route.abort('failed')
        }
      }
      if (method === 'POST' && url.pathname === historyPath && changeBeforeBoundRead
        && !req.postDataJSON()?.clear_readback) {
        // A real metadata write happens while the actual bound request waits.
        // The Host, not this harness, decides whether that observation is stale.
        changeBeforeBoundRead = false
        racePatch = await request('PATCH', `/api/v1/sessions/${id}`, { title: 'Synthetic success renamed' })
        assert.equal(racePatch.status, 200)
      }
      const safePost = method === 'POST' && [historyPath, clearPath, '/api/v1/supervisor/default',
        '/api/v1/tickets/search', '/api/v1/tickets/changes', '/api/v1/tickets/inspect'].includes(url.pathname)
      if (method === 'GET' || method === 'HEAD' || safePost) return route.continue()
      result.blocked.push({ scenario: label, method, origin: url.origin, path: url.pathname })
      return route.abort()
    })
    const selected = () => page.evaluate(async asset => {
      const module = await import(asset)
      const stores = Object.values(module).filter(x => typeof x === 'function' && typeof x.getState === 'function' && Array.isArray(x.getState()?.chats))
      if (stores.length !== 1) throw new Error(`Expected one chat store, got ${stores.length}`)
      return stores[0].getState().currentSessionId
    }, storeAsset)
    const uiState = () => page.evaluate(async ({ asset, sessionId }) => {
      const module = await import(asset)
      const stores = Object.values(module).filter(x => typeof x === 'function' && typeof x.getState === 'function' && Array.isArray(x.getState()?.chats))
      if (stores.length !== 1) throw new Error(`Expected one chat store, got ${stores.length}`)
      const state = stores[0].getState()
      return { selected: state.currentSessionId, binding: state.managedHistoryBindings?.[sessionId] ?? null,
        clear: state.conversationClears?.[sessionId] ?? null }
    }, { asset: storeAsset, sessionId: id })
    async function openManaged() {
      await page.getByRole('textbox', { name: 'Messages', exact: true }).first().waitFor({ state: 'visible', timeout: 30000 })
      await page.locator('aside').first().getByRole('button', { name: 'Supervisor', exact: true }).click()
      await page.getByRole('button', { name: 'Open workbench tab', exact: true }).click()
      await page.getByRole('menuitem', { name: 'Inspector', exact: true }).click()
      const row = page.getByTestId('supervisor-managed-panel').locator(`[data-managed-session="${id}"]`)
      await row.waitFor({ state: 'visible', timeout: 20000 })
      const open = row.getByRole('button', { name: 'Open', exact: true })
      assert.equal(await open.isEnabled(), true)
      const response = page.waitForResponse(r => new URL(r.url()).pathname === historyPath && r.request().method() === 'POST'
        && !r.request().postDataJSON()?.clear_readback, { timeout: 20000 })
      await open.click()
      const bound = await response
      assert.equal(bound.status(), 200)
      const body = await bound.json()
      const until = Date.now() + 15000
      while (await selected() !== id && Date.now() < until) await delay(100)
      assert.equal(await selected(), id)
      assert.equal(body.session_id, id)
      assert.equal(body.history_snapshot_source, 'durable')
      return body
    }
    try {
      await page.goto(base + '/', { waitUntil: 'domcontentloaded', timeout: 30000 })
      const before = await openManaged()
      check(`${label}: UI opens actual identity-bound synthetic history`, before.messages.some(m => m.content === marker))
      await page.getByText(marker, { exact: true }).first().waitFor({ state: 'visible', timeout: 10000 })
      if (!dropResponse) {
        changeBeforeBoundRead = true
        const staleRead = page.waitForResponse(r => new URL(r.url()).pathname === historyPath && r.status() === 409,
          { timeout: 20000 })
        const retriedRead = page.waitForResponse(r => new URL(r.url()).pathname === historyPath && r.request().method() === 'POST' && r.status() === 200
          && r.request().postDataJSON()?.target?.metadata_version > before.current_user_target.metadata_version,
        { timeout: 20000 })
        // Trigger the same reconciliation used by the live event feed; both
        // the conflict and retry are actual loopback HTTP responses.
        await page.evaluate(async asset => {
          const module = await import(asset)
          const stores = Object.values(module).filter(x => typeof x === 'function' && typeof x.getState === 'function' && Array.isArray(x.getState()?.chats))
          if (stores.length !== 1) throw new Error('Expected one chat store')
          const state = stores[0].getState()
          state.reconcileOpenSession(state.currentSessionId, 'message_appended')
        }, storeAsset)
        const [conflict, retry] = await Promise.all([staleRead, retriedRead])
        assert.equal((await conflict.json()).code, 'target_observation_stale')
        const rebound = await retry.json()
        assert.deepEqual(sameTarget(rebound.current_user_target), sameTarget(before.current_user_target))
        await waitForManagedBinding(async () => (await uiState()).binding, rebound.current_user_target)
        check('success: UI handles real 409 by re-reading the same identity', racePatch?.status === 200 && await selected() === id)
      }
      const sessionRow = page.locator(`[data-sidebar-session="${id}"]`)
      await sessionRow.getByRole('button', { name: 'Session actions', exact: true }).click()
      await page.getByRole('menuitem', { name: 'Clear conversation messages', exact: true }).click()
      const dialog = page.getByRole('dialog', { name: 'Clear this conversation?' })
      const readback = page.waitForResponse(r => new URL(r.url()).pathname === historyPath && r.request().method() === 'POST'
        && r.request().postDataJSON()?.clear_readback === true, { timeout: 20000 })
      if (dropResponse) {
        await dialog.getByRole('button', { name: 'Clear conversation messages', exact: true }).click()
        await dialog.getByText('Clear outcome unknown;', { exact: false }).waitFor({ state: 'visible', timeout: 20000 })
        assert.deepEqual(dropped, { status: 200, success: true, session_id: id })
        await dialog.getByRole('button', { name: 'Refresh stored state', exact: true }).click()
        const actualReadback = await readback
        assert.equal(actualReadback.status(), 200)
        assertEmptyReadback(before, await actualReadback.json())
        await dialog.getByText('Cleared message state verified. The earlier request’s outcome remains unknown;', { exact: false })
          .waitFor({ state: 'visible', timeout: 10000 })
        check('unknown: real readback does not claim the dropped write was acknowledged',
          await dialog.getByText('Clear outcome unknown;', { exact: false }).isVisible())
        check('unknown: only one real clear request was sent', clearCount === 1)
      } else {
        const clearResponse = page.waitForResponse(r => new URL(r.url()).pathname === clearPath && r.request().method() === 'POST', { timeout: 20000 })
        await dialog.getByRole('button', { name: 'Clear conversation messages', exact: true }).click()
        const [clear, actualReadback] = await Promise.all([clearResponse, readback])
        assert.equal(clear.status(), 200)
        const receipt = await clear.json()
        assert.equal(receipt.success, true)
        assert.equal(receipt.session_id, id)
        check('success: Host acknowledges exactly one C1 request', clearCount === 1)
        assert.equal(actualReadback.status(), 200)
        assertEmptyReadback(before, await actualReadback.json())
        check('success: Host returns complete identity-bound clear evidence', true)
        // A clear-feed event may retire an in-flight read. The UI deliberately
        // offers an explicit read-only refresh in that case; never resend C1.
        const settleBy = Date.now() + 10000
        let clearState
        do {
          clearState = (await uiState()).clear
          if (clearOutcome(clearState) !== 'waiting') break
          await delay(100)
        } while (Date.now() < settleBy)
        result.success_initial_clear_state = clearState
        if (clearOutcome(clearState) === 'refresh') {
          const retryReadback = page.waitForResponse(r => new URL(r.url()).pathname === historyPath
            && r.request().method() === 'POST' && r.request().postDataJSON()?.clear_readback === true,
          { timeout: 20000 })
          await dialog.getByRole('button', { name: 'Refresh stored state', exact: true }).click()
          const refreshedReadback = await retryReadback
          assert.equal(refreshedReadback.status(), 200)
          assertEmptyReadback(before, await refreshedReadback.json())
          check('success: explicit read-only refresh sent no second C1 request', clearCount === 1)
        } else {
          assert.equal(clearOutcome(clearState), 'confirmed', `unexpected initial C1 UI state: ${JSON.stringify(clearState)}`)
        }
        await dialog.getByText('Clearing acknowledged; stored messages and empty token usage verified.', { exact: false })
          .waitFor({ state: 'visible', timeout: 10000 })
        check('success: UI confirms one C1 receipt and complete bound readback', clearCount === 1)
        await dialog.getByRole('button', { name: 'Close', exact: true }).click()
        await page.reload({ waitUntil: 'domcontentloaded' })
        const reloaded = await openManaged()
        check('success: reload reopens the actual cleared bound history', reloaded.messages.every(m => m.content !== marker))
      }
      result[`${label}_screenshot_sha256`] = hash(await page.screenshot({ animations: 'disabled' }))
    } catch (error) {
      result[`${label}_failure_state`] = {
        ui: await uiState().catch(() => null),
        dialogs: await page.getByRole('dialog').allTextContents().catch(() => []),
        clear_requests: clearCount,
      }
      throw error
    } finally { await context.close() }
  }
  const scenarioFailures = await runIndependentScenarios([['success', false], ['unknown', true]], scenario,
    (label, error) => console.error(`FAIL ${label}:`, error))
  result.scenario_failures = scenarioFailures
  check('no browser requests escaped the loopback allowlist', result.blocked.length === 0)
  check('no uncaught browser page errors', result.page_errors.length === 0)
  check('all real Host scenarios completed', scenarioFailures.length === 0)
  result.status = 'PASS'
} catch (error) {
  result.status = 'FAIL'
  result.error = error instanceof Error ? error.stack : String(error)
  process.exitCode = 1
} finally {
  if (browser) await browser.close()
  if (!hostExit) {
    host.kill('SIGINT')
    const deadline = Date.now() + 15000
    while (!hostExit && Date.now() < deadline) await delay(100)
    if (!hostExit) { host.kill('SIGTERM'); await delay(1000) }
  }
  result.host_exit = hostExit
  if (!hostExit || hostExit.code !== 0) {
    result.status = 'FAIL'
    result.host_shutdown_error = 'The owned Host did not report a normal exit.'
    process.exitCode = 1
  }
  if (result.status !== 'PASS') {
    try { console.error((await readFile(path.join(root, 'host.log'), 'utf8')).split('\n').slice(-80).join('\n')) } catch { /* retain original failure */ }
  }
  console.log(JSON.stringify(result, null, 2))
  if (process.env.GITHUB_STEP_SUMMARY) await appendFile(process.env.GITHUB_STEP_SUMMARY,
    `## Bound history diagnostic: ${result.status}\n\n\`\`\`json\n${JSON.stringify(result, null, 2)}\n\`\`\`\n`)
}
