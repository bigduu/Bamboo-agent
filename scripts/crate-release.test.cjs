const assert = require('node:assert/strict')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { spawnSync } = require('node:child_process')
const { test } = require('node:test')
const { assertVersion, validateSource, validateReceipt, nextVersion, selectVersion,
  plan, publish, sha256, githubArgs, receiptBody, readBodyReceipt,
  frontendIdentity, verifyPackageArchive, preserveFrontend, verifyPreservedFrontend,
  shouldMakeLatest, gitIsAncestor, canonicalJson, signReceipt, authenticateReceipt,
  readAuthenticatedReceipt, assertSigningKey, verifyRegistrySource, highestStableVersion } = require('./crate-release.cjs')

const clone = (value) => JSON.parse(JSON.stringify(value))
const sourceRevision = 'a'.repeat(40)
const identity = { repository: 'bigduu/Bamboo-agent', sourceRevision,
  frontend: { packageName: '@bigduu/lotus-next', packageVersion: '2026.9.22',
    bundleHash: `sha256:${'b'.repeat(64)}`, lock: { sourceRevision: 'c'.repeat(40) } } }
const crates = ['bamboo-domain', 'bamboo-server', 'bamboo-agent']
const frontendBytes = { manifestSha256: 'd'.repeat(64), archiveSha256: 'e'.repeat(64) }
const checksum = 'f'.repeat(64)
const now = new Date('2026-10-07T12:00:00Z')
const signingKey = '7'.repeat(64)
const makeReceipt = (extra = {}) => signReceipt({ schemaVersion: 1, version: '2026.10.8',
  identity: clone(identity), crates: [...crates], frontendBytes: { ...frontendBytes },
  automatic: true, completed: false, ciRun: null, packageChecksums: {}, ...extra }, signingKey)
const release = (receipt, extra = {}) => ({ id: 42, tag_name: `v${receipt.version}`,
  target_commitish: receipt.identity.sourceRevision, draft: true, body: receiptBody(receipt), ...extra })

function fixture(extra = {}) {
  const calls = []
  const completions = []
  const bodies = []
  const published = new Map()
  const context = { automatic: true, identity: clone(identity), crates: [...crates],
    frontendBytes: { ...frontendBytes }, requestedVersion: '', sourceVersion: '0.0.0', now,
    releases: async () => [], receipts: [], tags: async () => [], versions: async () => [],
    readReceipt: async (entry, options) => readAuthenticatedReceipt(entry, signingKey, options),
    verifyReceipt: (receipt) => authenticateReceipt(receipt, signingKey),
    assertSigningKey: () => assertSigningKey(signingKey),
    tagSource: async () => sourceRevision,
    registryFrontier: async () => [],
    isAncestor: async () => { throw new Error('Unexpected source ancestry lookup') },
    reserve: async (receipt) => {
      calls.push('reserve')
      const entry = release(signReceipt(receipt, signingKey))
      bodies.push(entry.body)
      return entry
    },
    ensureTag: async () => { calls.push('tag') },
    ensureFrontend: async () => { calls.push('frontend') },
    saveReceipt: async (_entry, receipt) => {
      signReceipt(receipt, signingKey)
      bodies.push(receiptBody(receipt))
      calls.push(`save:${Object.keys(receipt.packageChecksums).length}:${receipt.completed}`)
    },
    registry: async (crate) => published.get(crate) || null,
    package: async (crate) => { calls.push(`package:${crate}`); return checksum },
    cargoPublish: async (crate) => { calls.push(`publish:${crate}`); published.set(crate, { checksum }); return { status: 0, output: '' } },
    verifyArchive: async (crate) => { calls.push(`verify:${crate}`) },
    wait: async () => { calls.push('wait') },
    complete: async (_entry, receipt, makeLatest) => { calls.push('complete'); completions.push({ version: receipt.version, makeLatest }) }, ...extra }
  return { context, calls, published, completions, bodies }
}

const completedReceipt = (version, revision) => makeReceipt({ version,
  identity: { ...clone(identity), sourceRevision: revision }, completed: true,
  packageChecksums: Object.fromEntries(crates.map((crate) => [crate, checksum])) })

test('automatic release accepts only the exact successful same-repository main push CI', () => {
  const base = { eventName: 'workflow_run', repository: identity.repository, sourceRevision,
    workflowRevision: 'c'.repeat(40), event: { workflow_run: { name: 'CI', event: 'push',
      conclusion: 'success', head_branch: 'main', head_sha: sourceRevision,
      repository: { full_name: identity.repository }, head_repository: { full_name: identity.repository } } } }
  assert.equal(validateSource(base), true)
  for (const [key, value] of [['name', 'Other'], ['event', 'pull_request'],
    ['conclusion', 'failure'], ['head_branch', 'dev'], ['head_sha', 'f'.repeat(40)],
    ['head_repository', { full_name: 'other/Bamboo-agent' }], ['repository', { full_name: 'other/Bamboo-agent' }]]) {
    const request = clone(base)
    request.event.workflow_run[key] = value
    assert.throws(() => validateSource(request), `${key} must be rejected`)
  }
  assert.equal(validateSource({ ...base, eventName: 'workflow_dispatch', workflowRevision: sourceRevision }), false)
  assert.throws(() => validateSource({ ...base, eventName: 'workflow_dispatch' }), /exact dispatched commit/)
  assert.throws(() => validateSource({ ...base, eventName: 'push' }), /Unsupported/)
})

test('date sequence advances beyond crate versions, tags and draft reservations in UTC', () => {
  assert.equal(nextVersion(['2026.10.7', '2026.10.12', '2026.9.200', 'v0.3.0'], now), '2026.10.13')
  assert.equal(nextVersion(['2026.9.200'], now), '2026.10.1')
  assert.equal(nextVersion(['2026.10.9'], new Date('2026-11-01T00:00:00Z')), '2026.11.1')
  assert.throws(() => nextVersion(['2026.10.999999999999999999999'], now), /safe integer/)
  for (const value of ['', '0.0.0', 'latest', '2026.10.8\nX=secret', '2026.10.8;false']) {
    assert.throws(() => assertVersion(value))
  }
})

test('automatic reruns reuse the source/frontend reservation and do not allocate another version', () => {
  const receipt = makeReceipt()
  const options = { automatic: true, identity, crates, releases: [release(receipt)], receipts: [receipt], versions: ['2026.10.90'], now }
  assert.equal(selectVersion(options), receipt.version)
  assert.throws(() => selectVersion({ ...options, receipts: [receipt, clone(receipt)] }), /Multiple automatic/)
  const changed = clone(identity)
  changed.frontend.bundleHash = `sha256:${'9'.repeat(64)}`
  assert.equal(selectVersion({ ...options, identity: changed }), '2026.10.91')
})

test('manual dispatch retains explicit versions but refuses unknown or mismatched occupied source', () => {
  const receipt = makeReceipt({ automatic: false })
  const options = { automatic: false, requestedVersion: receipt.version, sourceVersion: '0.0.0', identity, crates,
    releases: [release(receipt)], receipts: [receipt], versions: [receipt.version], now }
  assert.equal(selectVersion(options), receipt.version)
  assert.throws(() => selectVersion({ ...options, receipts: [] }), /provenance/)
  assert.throws(() => selectVersion({ ...options, releases: [], receipts: [] }), /occupied/)
  const foreign = makeReceipt()
  foreign.identity.sourceRevision = 'b'.repeat(40)
  assert.throws(() => selectVersion({ ...options, receipts: [foreign] }), /different source/)
  const differentFrontend = makeReceipt()
  differentFrontend.identity.frontend.packageName = '@bigduu/lotus'
  assert.throws(() => selectVersion({ ...options, receipts: [differentFrontend] }), /different source/)
  for (const requestedVersion of ['', 'latest', '0.0.0']) {
    assert.throws(() => selectVersion({ ...options, requestedVersion }), /placeholder/)
  }
})

test('receipt is included atomically at reservation and PATCH updates do not delete recovery state', () => {
  const receipt = makeReceipt()
  const reserved = release(receipt)
  assert.deepEqual(readBodyReceipt(reserved), receipt)
  assert.equal(readBodyReceipt({ body: 'Historical release without provenance' }), null)
  assert.deepEqual(githubArgs(identity.repository, 'releases', {}),
    ['gh', 'api', `repos/${identity.repository}/releases`, '--method', 'POST', '--input', '-'])
  assert.deepEqual(githubArgs(identity.repository, 'releases/42', {}, 'PATCH'),
    ['gh', 'api', `repos/${identity.repository}/releases/42`, '--method', 'PATCH', '--input', '-'])
  assert.throws(() => validateReceipt({ ...receipt, crates: ['bamboo-agent'] }, identity, crates), /closure changed/)
  assert.throws(() => validateReceipt({ ...receipt, completed: true }, identity, crates), /every package checksum/)
})

test('receipt authentication covers every payload field and extra JSON keys with stable object ordering', () => {
  const original = makeReceipt()
  assert.deepEqual(authenticateReceipt(clone(original), signingKey), original)
  const reordered = Object.fromEntries(Object.entries(original).reverse())
  reordered.identity = Object.fromEntries(Object.entries(reordered.identity).reverse())
  authenticateReceipt(reordered, signingKey)
  const mutations = [
    (r) => { r.schemaVersion = 2 }, (r) => { r.version = '2026.9999.9999' },
    (r) => { r.identity.repository = 'foreign/repo' }, (r) => { r.identity.sourceRevision = 'b'.repeat(40) },
    (r) => { r.identity.frontend.packageName = '@bigduu/lotus' },
    (r) => { r.identity.frontend.packageVersion = '2026.9.23' },
    (r) => { r.identity.frontend.bundleHash = `sha256:${'9'.repeat(64)}` },
    (r) => { r.identity.frontend.lock.sourceRevision = 'e'.repeat(40) },
    (r) => { r.crates.reverse() }, (r) => { r.frontendBytes.manifestSha256 = '8'.repeat(64) },
    (r) => { r.frontendBytes.archiveSha256 = '8'.repeat(64) },
    (r) => { r.packageChecksums['bamboo-domain'] = checksum },
    (r) => { r.automatic = false }, (r) => { r.completed = true }, (r) => { r.ciRun = 'https://example.invalid/ci' },
    (r) => { r.extra = true }, (r) => { r.identity.extra = true },
    (r) => { Object.defineProperty(r, '__proto__', { value: { compromised: true }, enumerable: true }) },
    (r) => { r.identity.constructor = { compromised: true } },
  ]
  for (const mutate of mutations) {
    const changed = clone(original)
    mutate(changed)
    assert.throws(() => authenticateReceipt(changed, signingKey), /authentication failed/)
  }
  const proto = JSON.parse('{"__proto__":{"x":1},"constructor":2}')
  assert.equal(canonicalJson(proto), '{"__proto__":{"x":1},"constructor":2}')
  assert.equal({}.x, undefined)
})

test('missing or malformed signing keys fail before external calls without revealing their value', async () => {
  for (const key of [undefined, '', 'private-key-should-not-appear', 'f'.repeat(63), 'G'.repeat(64)]) {
    const { context, calls } = fixture({ assertSigningKey: () => assertSigningKey(key),
      releases: async () => { throw new Error('Key failure must precede any external call') } })
    await assert.rejects(() => plan(context), (error) => {
      assert.match(error.message, /SIGNING_KEY/)
      if (key) assert.ok(!String(error.stack).includes(key))
      return true
    })
    assert.deepEqual(calls, [])
  }
})

test('missing, malformed, wrong-key and modified receipt authentication cannot be resumed', () => {
  const original = makeReceipt()
  for (const authentication of [undefined, { ...original.authentication, mac: 'f' },
    { ...original.authentication, mac: 'g'.repeat(64) }, { ...original.authentication, algorithm: 'none' },
    { ...original.authentication, extra: true }]) {
    assert.throws(() => authenticateReceipt({ ...original, authentication }, signingKey), /authentication/)
  }
  assert.throws(() => authenticateReceipt(original, '8'.repeat(64)), /authentication failed/)
  assert.deepEqual(readAuthenticatedReceipt(release(original), signingKey), original)
})

test('matching unsigned or bad-MAC automatic and manual drafts cannot be re-signed into a privileged publication', async () => {
  for (const version of ['2026.9999.9999', '2026.10.9999']) {
    for (const automatic of [true, false]) {
      for (const unsigned of [true, false]) {
        const forged = makeReceipt({ version, automatic })
        if (unsigned) delete forged.authentication
        else forged.authentication.mac = 'f'.repeat(64)
        const { context, calls } = fixture({ automatic, requestedVersion: automatic ? '' : version,
          releases: async () => [release(forged)] })
        await assert.rejects(() => plan(context), /authentication/)
        assert.deepEqual(calls, [])
      }
    }
  }
})

test('bad authenticated foreign history and accidental key replacement fail closed without privileged writes', async () => {
  const foreign = makeReceipt({ identity: { ...clone(identity), sourceRevision: 'b'.repeat(40) } })
  const { context, calls } = fixture({ releases: async () => [release(foreign)],
    readReceipt: async (entry, options) => readAuthenticatedReceipt(entry, '8'.repeat(64), options) })
  await assert.rejects(() => plan(context), /authentication failed/)
  assert.deepEqual(calls, [])
  foreign.authentication.mac = 'f'.repeat(64)
  assert.throws(() => readAuthenticatedReceipt(release(foreign), signingKey), /authentication failed/)
})

test('unrelated unsigned releases and huge bare tags are bounded occupancy without allocation or source-order authority', async () => {
  const forged = makeReceipt({ version: '2026.10.999999999999999999999999',
    identity: { ...clone(identity), sourceRevision: 'b'.repeat(40) }, completed: true,
    packageChecksums: Object.fromEntries(crates.map((crate) => [crate, checksum])) })
  delete forged.authentication
  const { context, completions } = fixture({ releases: async () => [release(forged, { id: 43, draft: false })],
    versions: async () => ['2026.10.7'], tags: async () => ['2026.10.8', '2026.10.9999', '2026.9999.9999'],
    tagSource: async () => { throw new Error('Unsigned history cannot authorize a source lookup') } })
  const result = await plan(context)
  assert.equal(result.receipt.version, '2026.10.9')
  await publish(context, result.release, result.receipt)
  assert.deepEqual(completions, [{ version: '2026.10.9', makeLatest: true }])
  const blocked = fixture({ versions: async () => ['2026.10.7'],
    tags: async () => Array.from({ length: 100 }, (_, i) => `2026.10.${i + 8}`) })
  await assert.rejects(() => plan(blocked.context), /Too many unauthenticated/)
  assert.deepEqual(blocked.calls, [])
})

test('authenticated history still rejects tag, target, repository and duplicate version replay', async () => {
  const receipt = makeReceipt()
  for (const changed of [release(receipt, { tag_name: 'v2026.10.99' }),
    release(receipt, { target_commitish: 'b'.repeat(40) }),
    release(makeReceipt({ identity: { ...clone(identity), repository: 'foreign/repo' } }))]) {
    const { context, calls } = fixture({ releases: async () => [changed] })
    await assert.rejects(() => plan(context))
    assert.deepEqual(calls, [])
  }
  const duplicate = fixture({ releases: async () => [release(receipt), release(receipt, { id: 43 })] })
  await assert.rejects(() => plan(duplicate.context), /Multiple authenticated reservations/)
  assert.deepEqual(duplicate.calls, [])
})

test('reservation and each partial/completed update persist a newly authenticated receipt', async () => {
  const { context, bodies } = fixture()
  const result = await plan(context)
  await publish(context, result.release, result.receipt)
  const receipts = bodies.map((body) => readAuthenticatedReceipt({ body }, signingKey, { required: true }))
  assert.deepEqual(receipts.map((r) => [Object.keys(r.packageChecksums).length, r.completed]),
    [[0, false], [1, false], [2, false], [3, false], [3, true]])
  assert.equal(new Set(receipts.map((r) => r.authentication.mac)).size, 5)
  const stale = { ...receipts[1], authentication: receipts[0].authentication }
  assert.throws(() => authenticateReceipt(stale, signingKey), /authentication failed/)
})

test('signed manual reservations retain explicit resume and cannot be relabeled as an automatic reservation', async () => {
  const manual = makeReceipt({ automatic: false })
  const { context, calls, completions } = fixture({ automatic: false, requestedVersion: manual.version,
    releases: async () => [release(manual)] })
  const resumed = await plan(context)
  await publish(context, resumed.release, resumed.receipt)
  assert.ok(!calls.includes('reserve'))
  assert.deepEqual(completions, [{ version: manual.version, makeLatest: false }])
  const automatic = fixture({ releases: async () => [release(manual)] })
  const planned = await plan(automatic.context)
  assert.equal(planned.receipt.version, '2026.10.9')
  assert.equal(planned.receipt.automatic, true)
})

test('dry run performs no registry/GitHub reads, draft reservation, tag or asset writes', async () => {
  const forbidden = async () => { throw new Error('External call during dry run') }
  const { context, calls } = fixture({ automatic: false, requestedVersion: '2026.10.8', dryRun: true,
    releases: forbidden, tags: forbidden, versions: forbidden, reserve: forbidden,
    ensureTag: forbidden, ensureFrontend: forbidden, saveReceipt: forbidden, assertSigningKey: forbidden })
  assert.deepEqual(await plan(context), { version: '2026.10.8', dryRun: true })
  assert.deepEqual(calls, [])
})

test('automatic planning trusts registry and signed reservations while tags only occupy candidates', async () => {
  const reserved = makeReceipt({ version: '2026.10.11', identity: { ...clone(identity), sourceRevision: 'b'.repeat(40) } })
  const { context, calls } = fixture({
    releases: async () => [release(reserved)],
    versions: async (names) => { assert.deepEqual(names, crates); return ['2026.10.9'] },
    tags: async () => ['2026.10.12'],
    tagSource: async () => reserved.identity.sourceRevision,
  })
  const result = await plan(context)
  assert.equal(result.receipt.version, '2026.10.13')
  assert.deepEqual(calls, ['reserve', 'tag', 'frontend'])
})

test('an interrupted draft creation resumes from its body before any asset upload', async () => {
  const receipt = makeReceipt()
  const { context, calls } = fixture({ releases: async () => [release(receipt)] })
  const result = await plan(context)
  assert.equal(result.receipt.version, receipt.version)
  assert.deepEqual(calls, ['tag', 'frontend'])
})

test('tag authority failure stops before frontend asset uploads or crate publication', async () => {
  const { context, calls } = fixture({ ensureTag: async () => { throw new Error('Workflows write authority required') } })
  await assert.rejects(() => plan(context), /Workflows write/)
  assert.deepEqual(calls, ['reserve'])
})

test('every package checksum is reserved before upload and every downloaded crate verified before public release', async () => {
  const { context, calls } = fixture()
  const receipt = makeReceipt()
  await publish(context, release(receipt), receipt)
  assert.deepEqual(calls, [
    'package:bamboo-domain', 'save:1:false', 'publish:bamboo-domain', 'verify:bamboo-domain',
    'package:bamboo-server', 'save:2:false', 'publish:bamboo-server', 'verify:bamboo-server',
    'package:bamboo-agent', 'save:3:false', 'publish:bamboo-agent', 'verify:bamboo-agent',
    'save:3:true', 'complete',
  ])
  assert.equal(receipt.completed, true)
})

test('partial upload resumes the same version without publishing or repackaging existing crates', async () => {
  const receipt = makeReceipt({ packageChecksums: { 'bamboo-domain': checksum } })
  const { context, calls, published } = fixture()
  published.set('bamboo-domain', { checksum })
  await publish(context, release(receipt), receipt)
  assert.equal(calls[0], 'verify:bamboo-domain')
  assert.ok(!calls.includes('package:bamboo-domain'))
  assert.ok(!calls.includes('publish:bamboo-domain'))
  assert.equal(calls.at(-1), 'complete')
})

test('unknown existing crate checksum and changed reserved package bytes fail without a public release', async () => {
  const first = fixture()
  first.published.set('bamboo-domain', { checksum })
  await assert.rejects(() => publish(first.context, release(makeReceipt()), makeReceipt()), /no reserved package checksum/)
  assert.deepEqual(first.calls, [])
  const second = fixture()
  second.published.set('bamboo-domain', { checksum: '8'.repeat(64) })
  const receipt = makeReceipt({ packageChecksums: { 'bamboo-domain': checksum } })
  await assert.rejects(() => publish(second.context, release(receipt), receipt), /Registry checksum mismatch/)
  assert.deepEqual(second.calls, [])
  const third = fixture({ package: async () => '8'.repeat(64) })
  await assert.rejects(() => publish(third.context, release(receipt), receipt), /Previously reserved package bytes changed/)
  assert.deepEqual(third.calls, [])
})

test('completed CI reruns verify all actual artifacts without changing latest or republishing', async () => {
  const receipt = makeReceipt({ completed: true, packageChecksums: Object.fromEntries(crates.map((crate) => [crate, checksum])) })
  const { context, calls, published } = fixture()
  for (const crate of crates) published.set(crate, { checksum })
  await publish(context, release(receipt, { draft: false }), receipt)
  assert.deepEqual(calls, crates.map((crate) => `verify:${crate}`))
})

test('older failed automatic reservations resume without replacing a newer main source as latest', async () => {
  const newerSource = 'b'.repeat(40)
  const older = makeReceipt({ packageChecksums: { 'bamboo-domain': checksum } })
  const newer = completedReceipt('2026.10.9', newerSource)
  const { context, published, completions } = fixture({
    releases: async () => [release(older), release(newer, { id: 43, draft: false })],
    tagSource: async (version) => version === older.version ? sourceRevision : newerSource,
    isAncestor: async (ancestor, descendant) => ancestor === sourceRevision && descendant === newerSource,
  })
  published.set('bamboo-domain', { checksum })
  const result = await plan(context)
  await publish(context, result.release, result.receipt)
  assert.deepEqual(completions, [{ version: older.version, makeLatest: false }])
})

test('older main CI first planned later cannot upload a higher version or become latest', async () => {
  const newerSource = 'b'.repeat(40)
  const newer = completedReceipt('2026.10.10', newerSource)
  const { context, completions, calls } = fixture({
    releases: async () => [release(newer, { id: 43, draft: false })],
    tagSource: async () => newerSource,
    isAncestor: async (ancestor, descendant) => ancestor === sourceRevision && descendant === newerSource,
  })
  const result = await plan(context)
  assert.equal(result.receipt.version, '2026.10.11')
  assert.equal(await shouldMakeLatest(context, result.release, result.receipt), false)
  await assert.rejects(() => publish(context, result.release, result.receipt), /cannot publish at or above/)
  assert.deepEqual(completions, [])
  assert.deepEqual(calls, ['reserve', 'tag', 'frontend'])
})

test('a newer main source becomes latest after a fresh completed-release ancestry check', async () => {
  const newerSource = 'b'.repeat(40)
  const older = completedReceipt('2026.10.9', sourceRevision)
  let reads = 0
  const { context, completions } = fixture({ identity: { ...clone(identity), sourceRevision: newerSource },
    releases: async () => ++reads === 1 ? [] : [release(older, { id: 43, draft: false })],
    tagSource: async () => sourceRevision,
    isAncestor: async (ancestor, descendant) => ancestor === sourceRevision && descendant === newerSource,
  })
  const result = await plan(context)
  await publish(context, result.release, result.receipt)
  assert.equal(reads, 3)
  assert.deepEqual(completions, [{ version: result.receipt.version, makeLatest: true }])
})

test('manual publication never changes latest or needs automatic source ordering', async () => {
  const { context, completions } = fixture({ automatic: false,
    releases: async () => { throw new Error('Manual completion must not query latest ordering') } })
  const receipt = makeReceipt({ automatic: false })
  await publish(context, release(receipt), receipt)
  assert.deepEqual(completions, [{ version: receipt.version, makeLatest: false }])
})

test('latest uses numeric versions only for the same source and rejects unproven completed tag identity', async () => {
  const current = makeReceipt({ version: '2026.10.9' })
  const other = completedReceipt('2026.10.10', sourceRevision)
  const { context } = fixture({ releases: async () => [release(other, { id: 43, draft: false })],
    tagSource: async () => sourceRevision })
  assert.equal(await shouldMakeLatest(context, release(current), current), false)
  current.version = '2026.10.11'
  assert.equal(await shouldMakeLatest(context, release(current), current), true)
  context.tagSource = async () => 'b'.repeat(40)
  await assert.rejects(() => shouldMakeLatest(context, release(current), current), /tag points to different source/)
  const divergent = completedReceipt('2026.10.1', 'b'.repeat(40))
  context.releases = async () => [release(divergent, { id: 43, draft: false })]
  context.tagSource = async () => divergent.identity.sourceRevision
  context.isAncestor = async () => false
  assert.equal(await shouldMakeLatest(context, release(current), current), false)
})

test('GitHub prereleases do not determine stable latest but their numeric crate versions still preserve source order', async () => {
  const newerSource = 'b'.repeat(40)
  const newer = completedReceipt('2026.10.10', newerSource)
  const { context, calls } = fixture({
    releases: async () => [release(newer, { id: 43, draft: false, prerelease: true })],
    tagSource: async () => newerSource, isAncestor: async () => false,
  })
  const current = makeReceipt({ version: '2026.10.11' })
  assert.equal(await shouldMakeLatest(context, release(current), current), true)
  await assert.rejects(() => publish(context, release(current), current), /cannot publish at or above/)
  assert.deepEqual(calls, [])
})

test('completed crate versions remain ordered even when the final GitHub publication left its Release draft', async () => {
  const newerSource = 'b'.repeat(40)
  const newer = completedReceipt('2026.10.10', newerSource)
  const { context, calls } = fixture({ releases: async () => [release(newer, { id: 43 })],
    tagSource: async () => newerSource, isAncestor: async () => false })
  const result = await plan(context)
  assert.equal(result.receipt.version, '2026.10.11')
  assert.equal(await shouldMakeLatest(context, result.release, result.receipt), true)
  await assert.rejects(() => publish(context, result.release, result.receipt), /cannot publish at or above/)
  assert.deepEqual(calls, ['reserve', 'tag', 'frontend'])
})

test('a newer main partial publication keeps its reserved version ahead of a late older main attempt', async () => {
  const newerSource = 'b'.repeat(40)
  const newer = makeReceipt({ version: '2026.10.10',
    identity: { ...clone(identity), sourceRevision: newerSource },
    packageChecksums: { 'bamboo-domain': checksum } })
  const { context, calls } = fixture({ releases: async () => [release(newer, { id: 43 })],
    tagSource: async () => newerSource, isAncestor: async () => false })
  const result = await plan(context)
  assert.equal(result.receipt.version, '2026.10.11')
  await assert.rejects(() => publish(context, result.release, result.receipt), /cannot publish at or above/)
  assert.deepEqual(calls, ['reserve', 'tag', 'frontend'])
})

test('deleting a newer receipt cannot let a late older source overtake verified registry artifacts', async () => {
  const newerSource = 'b'.repeat(40)
  for (const partial of [true, false]) {
    for (const missingMarker of [true, false]) {
      const newer = makeReceipt({ version: '2026.10.10', completed: !partial,
        identity: { ...clone(identity), sourceRevision: newerSource },
        packageChecksums: partial ? { 'bamboo-domain': checksum } : Object.fromEntries(crates.map((crate) => [crate, checksum])) })
      delete newer.authentication
      const { context, calls } = fixture({ versions: async () => ['2026.10.10'],
        releases: async () => [release(newer, { id: 43, draft: partial, ...(missingMarker ? { body: '' } : {}) })],
        registryFrontier: async () => [{ version: newer.version, sourceRevision: newerSource }], isAncestor: async () => false })
      const result = await plan(context)
      assert.equal(result.receipt.version, '2026.10.11')
      await assert.rejects(() => publish(context, result.release, result.receipt), /cannot overtake verified registry source/)
      assert.deepEqual(calls, ['reserve', 'tag', 'frontend'])
    }
  }
})

test('older reserved low versions can recover after history deletion without changing latest from newer registry source', async () => {
  const receipt = makeReceipt()
  const { context, completions } = fixture({ releases: async () => [release(receipt)],
    registryFrontier: async () => [{ version: '2026.10.9', sourceRevision: 'b'.repeat(40) }], isAncestor: async () => false })
  const result = await plan(context)
  await publish(context, result.release, result.receipt)
  assert.deepEqual(completions, [{ version: receipt.version, makeLatest: false }])
})

test('newer automatic sources cannot recover below older registry versions while same-source recovery is allowed', async () => {
  const olderSource = 'b'.repeat(40)
  const receipt = makeReceipt()
  const denied = fixture({ registryFrontier: async () => [{ version: '2026.10.9', sourceRevision: olderSource }],
    isAncestor: async () => true })
  await assert.rejects(() => publish(denied.context, release(receipt), receipt), /cannot publish below verified registry version/)
  assert.deepEqual(denied.calls, [])
  const allowed = fixture({ registryFrontier: async () => [{ version: '2026.10.9', sourceRevision }],
    isAncestor: async () => { throw new Error('Identical source needs no ancestry lookup') } })
  await publish(allowed.context, release(receipt), receipt)
  assert.deepEqual(allowed.completions, [{ version: receipt.version, makeLatest: false }])
})

test('manual recovery of an originally automatic reservation preserves its signature without claiming latest', async () => {
  const receipt = makeReceipt()
  const { context, completions } = fixture({ automatic: false, requestedVersion: receipt.version,
    releases: async () => [release(receipt)], registryFrontier: async () => { throw new Error('Manual recovery needs no automatic ordering') } })
  const result = await plan(context)
  await publish(context, result.release, result.receipt)
  assert.equal(result.receipt.automatic, true)
  authenticateReceipt(result.receipt, signingKey)
  assert.deepEqual(completions, [{ version: receipt.version, makeLatest: false }])
})

test('registry source selection is refreshed at completion and uses numeric stable versions across months', async () => {
  assert.equal(highestStableVersion(['2026.9.9999', '2026.10.1', '2026.11.1-beta', '0.3.0']), '2026.10.1')
  assert.equal(highestStableVersion(['2026.11.1-beta']), null)
  let reads = 0
  const { context, completions } = fixture({ registryFrontier: async () => ++reads === 1 ? [] :
    [{ version: '2026.10.9', sourceRevision: 'b'.repeat(40) }], isAncestor: async () => false })
  const receipt = makeReceipt()
  await publish(context, release(receipt), receipt)
  assert.equal(reads, 2)
  assert.deepEqual(completions, [{ version: receipt.version, makeLatest: false }])
})

test('Git source ordering handles descendants published after checkout and rejects divergent history', (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'bamboo-release-ancestry-'))
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }))
  const origin = path.join(directory, 'origin')
  const checkout = path.join(directory, 'checkout')
  fs.mkdirSync(origin)
  const git = (...args) => {
    const result = spawnSync('git', args, { cwd: origin, encoding: 'utf8' })
    assert.equal(result.status, 0, result.stderr)
    return result.stdout.trim()
  }
  git('init', '-q')
  const commit = (contents) => {
    fs.writeFileSync(path.join(origin, 'source'), contents)
    git('add', 'source')
    git('-c', 'user.name=Release fixture', '-c', 'user.email=fixture@example.invalid', 'commit', '-qm', contents)
    return git('rev-parse', 'HEAD')
  }
  const older = commit('older main')
  git('clone', '--no-local', '-q', origin, checkout)
  const newer = commit('newer main')
  assert.equal(gitIsAncestor(older, newer, checkout), true)
  assert.equal(gitIsAncestor(newer, older, checkout), false)
  assert.equal(gitIsAncestor(newer, newer, checkout), true)
  git('checkout', '-qb', 'divergent', older)
  const divergent = commit('rewritten main')
  assert.equal(gitIsAncestor(newer, divergent, checkout), false)
  assert.throws(() => gitIsAncestor(older, '0'.repeat(40), checkout), /failed/)
})

test('rate limiting is retried, while fatal publish and registry failures keep the release draft', async () => {
  let attempts = 0
  const { context, calls, published } = fixture({ cargoPublish: async (crate) => {
    if (++attempts === 1) return { status: 1, output: '429 Too Many Requests' }
    published.set(crate, { checksum }); return { status: 0, output: '' }
  } })
  await publish(context, release(makeReceipt()), makeReceipt())
  assert.ok(calls.includes('wait'))
  const fatal = fixture({ cargoPublish: async () => ({ status: 1, output: 'compiler failure' }) })
  await assert.rejects(() => publish(fatal.context, release(makeReceipt()), makeReceipt()), /compiler failure/)
  assert.ok(!fatal.calls.includes('complete'))
  const unavailable = fixture({ registry: async () => { throw new Error('Registry HTTP 503') } })
  await assert.rejects(() => publish(unavailable.context, release(makeReceipt()), makeReceipt()), /503/)
  assert.deepEqual(unavailable.calls, [])
})

test('downloaded crate bytes, source revision and server frontend must all match the reserved evidence', () => {
  const manifest = Buffer.from('{"built_at":"2026-10-07T00:00:00Z"}')
  const zip = Buffer.from('original staged zip bytes')
  const bytes = Buffer.from('crate archive bytes')
  const receipt = makeReceipt({ frontendBytes: { manifestSha256: sha256(manifest), archiveSha256: sha256(zip) } })
  const entries = { '.cargo_vcs_info.json': Buffer.from(JSON.stringify({ git: { sha1: sourceRevision, dirty: true } })),
    'frontend_package/frontend-manifest.json': manifest, 'frontend_package/lotus-frontend.zip': zip }
  const entry = (name) => entries[name]
  verifyPackageArchive(bytes, receipt, 'bamboo-server', sha256(bytes), entry)
  assert.throws(() => verifyPackageArchive(Buffer.from('foreign bytes'), receipt, 'bamboo-server', sha256(bytes), entry), /checksum mismatch/)
  entries['.cargo_vcs_info.json'] = Buffer.from(JSON.stringify({ git: { sha1: '9'.repeat(40) } }))
  assert.throws(() => verifyPackageArchive(bytes, receipt, 'bamboo-server', sha256(bytes), entry), /source revision mismatch/)
  entries['.cargo_vcs_info.json'] = Buffer.from(JSON.stringify({ git: { sha1: sourceRevision } }))
  entries['frontend_package/lotus-frontend.zip'] = Buffer.from('different frontend')
  assert.throws(() => verifyPackageArchive(bytes, receipt, 'bamboo-server', sha256(bytes), entry))
})

test('real staged frontend timestamps change but a retry restores the reserved original bytes', async (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'bamboo-release-stage-'))
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }))
  const scriptDirectory = path.join(directory, 'scripts')
  fs.mkdirSync(scriptDirectory)
  fs.mkdirSync(path.join(directory, "crates/app/bamboo-server"), { recursive: true })
  for (const name of ['frontend-package.cjs', 'lotus-next-artifact.cjs']) {
    fs.copyFileSync(path.join(__dirname, name), path.join(scriptDirectory, name))
  }
  const dist = path.join(directory, 'node_modules/@bigduu/lotus/dist')
  fs.mkdirSync(dist, { recursive: true })
  fs.writeFileSync(path.join(dist, 'index.html'), '<main>fixed frontend fixture</main>\n')
  fs.writeFileSync(path.join(path.dirname(dist), 'package.json'), JSON.stringify({ name: '@bigduu/lotus', version: '2026.8.28' }))
  const stage = () => {
    const result = spawnSync(process.execPath, [path.join(scriptDirectory, 'frontend-package.cjs'), 'stage'], {
      cwd: directory, encoding: 'utf8', env: { ...process.env, LOTUS_SOURCE: 'package',
        LOTUS_PACKAGE_NAME: '@bigduu/lotus', LOTUS_VERSION: '2026.8.28' },
    })
    assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`)
    const root = path.join(directory, 'crates/app/bamboo-server/frontend_package')
    return { manifest: JSON.parse(fs.readFileSync(path.join(root, 'frontend-manifest.json'), 'utf8')),
      zip: fs.readFileSync(path.join(root, 'lotus-frontend.zip')) }
  }
  const first = stage()
  const second = stage()
  assert.notEqual(first.manifest.built_at, second.manifest.built_at)
  assert.notEqual(sha256(first.zip), sha256(second.zip))
  assert.deepEqual(frontendIdentity('@bigduu/lotus', '2026.8.28', first.manifest, null),
    frontendIdentity('@bigduu/lotus', '2026.8.28', second.manifest, null))
  const firstManifest = Buffer.from(`${JSON.stringify(first.manifest, null, 2)}\n`)
  const receipt = makeReceipt({ frontendBytes: { manifestSha256: sha256(firstManifest), archiveSha256: sha256(first.zip) },
    packageChecksums: { 'bamboo-domain': checksum } })
  let restored
  await preserveFrontend({ receipt, stagedBytes: { manifestSha256: '9'.repeat(64), archiveSha256: '8'.repeat(64) },
    assets: [firstManifest, first.zip], readAsset: async (asset) => asset,
    verifyFiles: async (bytes) => verifyPreservedFrontend([Buffer.from(`${JSON.stringify(second.manifest, null, 2)}\n`), second.zip], bytes),
    writeFiles: async (bytes) => { restored = bytes },
    saveReceipt: async () => { throw new Error('Must not change reserved bytes') },
    upload: async () => { throw new Error('Must not overwrite preserved assets') },
  })
  assert.deepEqual(restored, [firstManifest, first.zip])
  const trusted = [Buffer.from(`${JSON.stringify(second.manifest, null, 2)}\n`), second.zip]
  for (const field of ['schema_version', 'frontend_name', 'frontend_version', 'bundle_hash', 'entry']) {
    const changed = { ...first.manifest, [field]: 'foreign value' }
    assert.throws(() => verifyPreservedFrontend(trusted,
      [Buffer.from(`${JSON.stringify(changed, null, 2)}\n`), first.zip]), /manifest differs/)
  }
  const originalZip = path.join(directory, 'original.zip')
  fs.writeFileSync(originalZip, first.zip)
  const mutate = 'from zipfile import ZipFile,ZipInfo\nimport sys,stat\n' +
    'mode=sys.argv[3]\n' +
    'with ZipFile(sys.argv[1]) as original, ZipFile(sys.argv[2],"w") as changed:\n' +
    ' for info in original.infolist():\n' +
    '  data=original.read(info)\n' +
    '  if mode=="payload" and info.filename=="index.html": data=data.replace(b"fixed",b"evil!")\n' +
    '  if mode=="manifest" and info.filename=="frontend-manifest.json": data=data.replace(b"2026",b"2025")\n' +
    '  if mode=="symlink" and info.filename=="index.html": info.external_attr=(stat.S_IFLNK|0o777)<<16\n' +
    '  changed.writestr(info,data)\n' +
    ' if mode=="extra": changed.writestr("evil.js",b"malicious")\n' +
    ' if mode=="duplicate": changed.writestr("index.html",b"malicious")\n'
  for (const [mode, message] of [['payload', /ZIP payload differs/], ['manifest', /embedded manifest differs/],
    ['symlink', /ZIP entry type differs/], ['extra', /ZIP entry inventory differs/], ['duplicate', /ZIP entry inventory differs/]]) {
    const changedZip = path.join(directory, `${mode}.zip`)
    const mutation = spawnSync('python3', ['-c', mutate, originalZip, changedZip, mode], { encoding: 'utf8' })
    assert.equal(mutation.status, 0, mutation.stderr)
    const malicious = [firstManifest, fs.readFileSync(changedZip)]
    const forged = makeReceipt({ frontendBytes: { manifestSha256: sha256(malicious[0]), archiveSha256: sha256(malicious[1]) } })
    let wrote = false
    await assert.rejects(() => preserveFrontend({ receipt: forged, stagedBytes: frontendBytes,
      assets: malicious, readAsset: async (asset) => asset,
      verifyFiles: async (bytes) => verifyPreservedFrontend(trusted, bytes),
      writeFiles: async () => { wrote = true }, saveReceipt: async () => { wrote = true }, upload: async () => { wrote = true },
    }), message)
    assert.equal(wrote, false, `${mode}: self-consistent forged receipt must not replace trusted staging`)
  }
})

test('interrupted initial frontend upload can recover before any crate, while packaged releases never adopt changed bytes', async () => {
  const receipt = makeReceipt()
  const calls = []
  const options = { receipt, stagedBytes: { manifestSha256: '9'.repeat(64), archiveSha256: '8'.repeat(64) },
    assets: [Buffer.from('old partial manifest'), null], readAsset: async (asset) => asset,
    writeFiles: async () => { throw new Error('Should replace initial assets') },
    saveReceipt: async () => { calls.push('atomic-body') }, upload: async () => { calls.push('upload') } }
  await preserveFrontend(options)
  assert.deepEqual(calls, ['atomic-body', 'upload'])
  assert.deepEqual(receipt.frontendBytes, options.stagedBytes)
  receipt.packageChecksums['bamboo-domain'] = checksum
  await assert.rejects(() => preserveFrontend(options), /lost or changed/)
  await assert.rejects(() => preserveFrontend({ ...options, assets: [Buffer.from('foreign manifest'), Buffer.from('foreign zip')] }), /lost or changed/)
  assert.deepEqual(calls, ['atomic-body', 'upload'])
})

test('workflow preserves the exact CI commit, shared publication queue and separate manual CI queue', () => {
  const workflow = fs.readFileSync('.github/workflows/publish-crate.yml', 'utf8')
  assert.match(workflow, /workflow_run:\n    workflows: \[CI\]\n    types: \[completed\]\n    branches: \[main\]/)
  assert.match(workflow, /group: bamboo-crate-publication\n  queue: max\n  cancel-in-progress: false/)
  assert.match(workflow, /ref: \$\{\{ github.event_name == 'workflow_run' && github.event.workflow_run.head_sha \|\| github.sha \}\}/)
  assert.match(workflow, /contents: write/)
  assert.match(workflow, /secrets.BAMBOO_RELEASE_TOKEN \|\| github.token/)
  assert.equal((workflow.match(/BAMBOO_RELEASE_SIGNING_KEY: \$\{\{ secrets.BAMBOO_RELEASE_SIGNING_KEY \}\}/g) || []).length, 2)
  assert.doesNotMatch(workflow, /VERSION="\$\{\{ github.event.inputs.version/)
  const ci = fs.readFileSync('.github/workflows/ci.yml', 'utf8')
  assert.match(ci, /group: ci-\$\{\{ github.event_name \}\}/)
  assert.match(ci, /queue: \$\{\{ github.event_name == 'push' && github.ref == 'refs\/heads\/main' && 'max' \|\| 'single' \}\}/)
  assert.match(ci, /cancel-in-progress: \$\{\{ github.event_name != 'push' \|\| github.ref != 'refs\/heads\/main' \}\}/)
  assert.match(ci, /node --test scripts\/ci-policy.test.cjs scripts\/crate-release.test.cjs/)
})

test('temporary manifest stamping uses exact internal dependency versions and real package versions', (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'bamboo-release-stamp-'))
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }))
  const member = path.join(directory, 'crates/core/bamboo-domain')
  fs.mkdirSync(member, { recursive: true })
  fs.writeFileSync(path.join(directory, 'Cargo.toml'),
    '[workspace.package]\nversion = "0.0.0"\n[package]\nname = "bamboo-agent"\nversion.workspace = true\n[dependencies]\n' +
    'bamboo-domain = { path = "crates/core/bamboo-domain", version = "0.0.0" }\n' +
    '[build-dependencies]\nbamboo-domain = { path = "crates/core/bamboo-domain" }\n')
  fs.writeFileSync(path.join(member, 'Cargo.toml'), '[package]\nname = "bamboo-domain"\nversion.workspace = true\n')
  const workflow = fs.readFileSync('.github/workflows/publish-crate.yml', 'utf8')
  const inline = workflow.split("          python3 - <<'PY'\n")[1].split('\n          PY')[0]
    .split('\n').map((line) => line.replace(/^ {10}/, '')).join('\n')
  const python = ['python3', 'python3.12', 'python3.14'].find((candidate) =>
    spawnSync(candidate, ['-c', 'import tomllib']).status === 0)
  assert.ok(python, 'Python >= 3.11 is required by the publication manifest policy')
  const result = spawnSync(python, ['-c', inline], { cwd: directory, encoding: 'utf8',
    env: { ...process.env, TARGET_VERSION: '2026.10.8' } })
  assert.equal(result.status, 0, result.stderr)
  const stamped = fs.readFileSync(path.join(directory, 'Cargo.toml'), 'utf8')
  assert.match(stamped, /\[workspace.package\]\nversion = "2026.10.8"/)
  assert.equal((stamped.match(/version = "=2026.10.8"/g) || []).length, 2)
  assert.doesNotMatch(stamped, /\[workspace.package\]\nversion = "=/)
  assert.match(fs.readFileSync('Cargo.toml', 'utf8'), /^version = "0.0.0"$/m)
  const runner = fs.readFileSync('scripts/crate-release.cjs', 'utf8')
  assert.match(runner, /'package', '--locked', '--allow-dirty', '--no-verify'/)
  assert.match(runner, /'publish', '--locked', '--allow-dirty'/)
})

test('stamped source lockfile is refreshed before the first locked package', (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'bamboo-release-lock-'))
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }))
  fs.mkdirSync(path.join(directory, 'src'))
  fs.writeFileSync(path.join(directory, 'src/lib.rs'), 'pub fn fixture() -> u32 { 1 }\n')
  const manifest = path.join(directory, 'Cargo.toml')
  fs.writeFileSync(manifest, '[package]\nname="bamboo-release-lock-fixture"\nversion="0.0.0"\nedition="2021"\nlicense="MIT"\n')
  const run = (args) => spawnSync(args[0], args.slice(1), { cwd: directory, encoding: 'utf8' })
  const success = (args) => { const result = run(args); assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`); return result }
  success(['cargo', 'generate-lockfile', '--offline'])
  success(['git', 'init', '-q'])
  success(['git', 'add', '.'])
  success(['git', '-c', 'user.name=Release fixture', '-c', 'user.email=fixture@example.invalid', 'commit', '-qm', 'source'])
  const initial = fs.readFileSync(path.join(directory, 'Cargo.lock'), 'utf8')
  fs.writeFileSync(manifest, fs.readFileSync(manifest, 'utf8').replace('0.0.0', '2026.10.8'))
  success(['cargo', 'metadata', '--format-version', '1', '--no-deps', '--offline'])
  assert.equal(fs.readFileSync(path.join(directory, 'Cargo.lock'), 'utf8'), initial)
  const packageArgs = ['cargo', 'package', '--locked', '--allow-dirty', '--no-verify', '--offline']
  const stale = run(packageArgs)
  assert.notEqual(stale.status, 0)
  assert.match(stale.stderr, /lock file.*--locked/s)
  const workflow = fs.readFileSync('.github/workflows/publish-crate.yml', 'utf8')
  const marker = '      - name: Refresh the temporary stamped publication lockfile'
  assert.ok(workflow.indexOf('      - name: Prepare workspace manifests for publish') < workflow.indexOf(marker))
  assert.ok(workflow.indexOf(marker) < workflow.indexOf('      - name: Publish verified crates'))
  assert.match(workflow, /cargo metadata --format-version 1 > \/dev\/null\n          python3 scripts\/crate-release-lock.py/)
  const resolved = JSON.parse(success(['cargo', 'metadata', '--format-version', '1', '--offline']).stdout)
  const python = ['python3', 'python3.12', 'python3.14'].find((candidate) => spawnSync(candidate, ['-c', 'import tomllib']).status === 0)
  success([python, path.join(__dirname, 'crate-release-lock.py')])
  success(packageArgs)
  assert.match(fs.readFileSync(path.join(directory, 'Cargo.lock'), 'utf8'), /version = "2026.10.8"/)
  const archive = path.join(resolved.target_directory, 'package/bamboo-release-lock-fixture-2026.10.8.crate')
  const bytes = fs.readFileSync(archive)
  const entry = (name) => {
    const result = spawnSync('tar', ['-xOf', archive, `bamboo-release-lock-fixture-2026.10.8/${name}`])
    assert.equal(result.status, 0, result.stderr.toString())
    return result.stdout
  }
  assert.equal(verifyRegistrySource(bytes, sha256(bytes), entry), success(['git', 'rev-parse', 'HEAD']).stdout.trim())
  assert.throws(() => verifyRegistrySource(bytes, '0'.repeat(64), entry), /checksum mismatch/)
  assert.throws(() => verifyRegistrySource(bytes, sha256(bytes), () => Buffer.from('{"git":{}}')))
  assert.throws(() => verifyRegistrySource(bytes, sha256(bytes), () => Buffer.from('{"git":{"sha1":"invalid"}}')))
})

test('temporary lock refresh preserves the exact tested external dependency multiset', () => {
  const python = ['python3', 'python3.12', 'python3.14'].find((candidate) => spawnSync(candidate, ['-c', 'import tomllib']).status === 0)
  assert.ok(python)
  const external = { name: 'external', version: '1.0.0', source: 'registry+https://example.invalid/index', checksum: 'a'.repeat(64) }
  const source = { package: [{ name: 'workspace', version: '0.0.0' }, external] }
  const stamped = { package: [clone(external), { name: 'workspace', version: '2026.10.8' }] }
  const script = 'import importlib.util,json,sys\n' +
    'spec=importlib.util.spec_from_file_location("lock_policy",sys.argv[1])\n' +
    'module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)\n' +
    'module.assert_external_lock_unchanged(json.loads(sys.argv[2]),json.loads(sys.argv[3]))\n'
  const check = (candidate) => spawnSync(python, ['-c', script, path.join(__dirname, 'crate-release-lock.py'),
    JSON.stringify(source), JSON.stringify(candidate)], { encoding: 'utf8' })
  assert.equal(check(stamped).status, 0)
  for (const field of ['name', 'version', 'source', 'checksum']) {
    const changed = clone(stamped)
    changed.package[0][field] += '-changed'
    const result = check(changed)
    assert.notEqual(result.status, 0)
    assert.match(result.stderr, /External dependency lock changed/)
  }
  assert.notEqual(check({ package: [stamped.package[1]] }).status, 0)
  assert.notEqual(check({ package: [...stamped.package, clone(external)] }).status, 0)
})
