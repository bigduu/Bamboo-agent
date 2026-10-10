const assert = require('node:assert/strict')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { spawnSync } = require('node:child_process')
const { test } = require('node:test')
const { assertVersion, assertManualVersion, validateSource, validateReceipt, nextVersion, selectVersion,
  plan, publish, sha256, githubArgs, receiptBody, readBodyReceipt,
  frontendIdentity, verifyPackageArchive, preserveFrontend, verifyPreservedFrontend,
  shouldMakeLatest, gitIsAncestor, canonicalJson, signReceipt, authenticateReceipt,
  readAuthenticatedReceipt, assertSigningKey, assertSigningConfiguration, verifyRegistrySource, highestStableVersion,
  command, spawnCommand } = require('./crate-release.cjs')

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
const signingKeyDigest = sha256(Buffer.from(signingKey, 'hex'))
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
    frontendBytes: { ...frontendBytes }, now,
    releases: async () => [], receipts: [], tags: async () => [], versions: async () => [],
    readReceipt: async (entry, options) => readAuthenticatedReceipt(entry, signingKey, options),
    verifyReceipt: (receipt) => authenticateReceipt(receipt, signingKey),
    assertSigningKey: () => assertSigningConfiguration(signingKey, signingKeyDigest),
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
  assert.throws(() => validateSource({ ...base, eventName: 'workflow_dispatch' }), /Unsupported/)
  assert.throws(() => validateSource({ ...base, eventName: 'push' }), /Unsupported/)
})

test('date sequence advances beyond crate versions, tags and draft reservations in UTC', () => {
  assert.equal(nextVersion(['2026.10.7', '2026.10.12', '2026.9.200', 'v0.3.0'], now), '2026.10.13')
  assert.equal(nextVersion(['2026.9.200'], now), '2026.10.1')
  assert.equal(nextVersion(['2026.10.9'], new Date('2026-11-01T00:00:00Z')), '2026.11.1')
  assert.throws(() => nextVersion(['2026.10.999999999999999999999'], now), /u64/)
  for (const value of ['', '0.0.0', 'latest', '2026.10.8\nX=secret', '2026.10.8;false']) {
    assert.throws(() => assertVersion(value))
  }
})

test('automatic counters preserve the full Cargo u64 range and stop before reserving an exhausted month', async () => {
  assert.equal(nextVersion(['2026.10.9007199254740992'], now), '2026.10.9007199254740993')
  assert.equal(nextVersion(['2026.10.9007199254740992'], now, ['2026.10.9007199254740993']),
    '2026.10.9007199254740994')
  assert.equal(nextVersion(['2026.10.18446744073709551614'], now), '2026.10.18446744073709551615')
  assert.equal(assertVersion('2026.10.18446744073709551615'), '2026.10.18446744073709551615')
  assert.equal(nextVersion(['2026.10.18446744073709551615'], new Date('2026-11-01T00:00:00Z')), '2026.11.1')
  for (const [versions, tags] of [
    [['2026.10.18446744073709551615'], []],
    [['2026.10.18446744073709551614'], ['2026.10.18446744073709551615']],
  ]) {
    const { context, calls } = fixture({ versions: async () => versions, tags: async () => tags })
    await assert.rejects(() => plan(context), /Cargo u64 limit/)
    assert.deepEqual(calls, [], 'Exhaustion must not reserve a release, tag or upload')
  }
  const historical = makeReceipt({ automatic: false, version: '2026.10.9007199254740992' })
  const automatic = fixture({ releases: async () => [release(historical)], tagSource: async () => sourceRevision })
  assert.equal((await plan(automatic.context)).receipt.version, '2026.10.9007199254740993')
})

test('real subprocesses receive only their own publication credentials', (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'bamboo-release-child-env-'))
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }))
  const githubAuth = ['GH_TOKEN', 'GITHUB_TOKEN', 'GH_ENTERPRISE_TOKEN', 'GITHUB_ENTERPRISE_TOKEN']
  const credentials = [...githubAuth, 'GH_AUTH_TOKEN', 'GITHUB_PAT', 'GH_PAT', 'gh_token',
    'BAMBOO_RELEASE_TOKEN', 'BAMBOO_RELEASE_SIGNING_KEY', 'BAMBOO_RELEASE_SIGNING_KEY_SHA256',
    'CARGO_REGISTRY_TOKEN', 'CARGO_REGISTRIES_OTHER_TOKEN']
  const executable = `#!${process.execPath}\n` +
    `const names = ${JSON.stringify(credentials)};\n` +
    'console.log(JSON.stringify({ credentials: names.filter(name => process.env[name] !== undefined).sort(),\n' +
    '  setting: process.env.NORMAL_SETTING, cargoHome: process.env.CARGO_HOME, rustupHome: process.env.RUSTUP_HOME }));\n'
  for (const name of ['cargo', 'gh', 'git', 'python3', 'tar']) {
    fs.writeFileSync(path.join(directory, name), executable, { mode: 0o755 })
  }
  const env = { PATH: `${directory}${path.delimiter}${process.env.PATH}`, HOME: directory,
    CARGO_HOME: directory, RUSTUP_HOME: directory, NORMAL_SETTING: 'retained',
    ...Object.fromEntries(credentials.map(name => [name, `dummy-${name}`])) }
  const check = (args, expected, direct = false) => {
    const output = direct ? spawnCommand(args, { env }).stdout : command(args, { env })
    assert.deepEqual(JSON.parse(output), { credentials: [...expected].sort(), setting: 'retained',
      cargoHome: directory, rustupHome: directory }, args.join(' '))
  }
  for (const action of ['metadata', 'package', 'check', 'test']) check(['cargo', action, '--locked'], [])
  check(['cargo', 'publish', '--locked'], ['CARGO_REGISTRY_TOKEN'], true)
  check(['gh', 'api', 'repos/fixture/releases'], githubAuth)
  for (const args of [['git', 'cat-file'], ['git', 'merge-base'], ['python3', 'compute-publish-order.py'], ['tar', '-xOf']]) {
    check(args, [])
  }
  const source = fs.readFileSync(path.join(__dirname, 'crate-release.cjs'), 'utf8')
  assert.equal([...source.matchAll(/\bspawnSync\(/g)].length, 1, 'All direct child launches must use the credential filter')
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

test('unauthenticated drafts targeting the public current SHA only occupy their names during automatic publication', async () => {
  for (const version of ['2026.10.8', '2026.10.9999']) {
    for (const mode of ['missing', 'unsigned', 'bad-mac', 'malformed-json']) {
      const forged = makeReceipt({ version })
      if (mode === 'unsigned') delete forged.authentication
      else forged.authentication.mac = 'f'.repeat(64)
      const body = mode === 'missing' ? 'No canonical receipt' : mode === 'malformed-json'
        ? '<!-- bamboo-release-provenance\n{\n-->' : receiptBody(forged)
      const entry = release(forged, { id: 43, body })
      const original = clone(entry)
      const { context, calls, bodies, completions } = fixture({ releases: async () => [entry],
        versions: async () => ['2026.10.7'],
        tagSource: async () => { throw new Error('Unauthenticated target cannot authorize a tag lookup') } })
      const result = await plan(context)
      const expectedVersion = version === '2026.10.8' ? '2026.10.9' : '2026.10.8'
      assert.equal(result.receipt.version, expectedVersion)
      await publish(context, result.release, result.receipt)
      assert.deepEqual(completions, [{ version: expectedVersion, makeLatest: true }])
      assert.equal(calls.filter(call => call === 'reserve').length, 1)
      assert.deepEqual(entry, original, 'The attacker record must never be modified or re-signed')
      assert.ok(bodies.every(body => readAuthenticatedReceipt({ body }, signingKey).version === expectedVersion))
    }
  }
})

test('protected signing configuration rejects key drift before any history or publication call', async () => {
  assertSigningConfiguration(signingKey, signingKeyDigest)
  const configurations = [
    ['8'.repeat(64), signingKeyDigest], [signingKey, undefined], [signingKey, ''],
    [signingKey, 'F'.repeat(64)], [signingKey, 'f'.repeat(63)],
    [signingKey, sha256(Buffer.from(signingKey))],
  ]
  for (const [key, digest] of configurations) {
    let externalCalls = 0
    const { context, calls } = fixture({
      assertSigningKey: () => assertSigningConfiguration(key, digest),
      releases: async () => { externalCalls++; return [] },
      registryFrontier: async () => { externalCalls++; return [] },
    })
    const receipt = makeReceipt()
    for (const action of [() => plan(context), () => publish(context, release(receipt), receipt),
      () => shouldMakeLatest(context, release(receipt), receipt)]) {
      await assert.rejects(action, (error) => {
        assert.match(error.message, /configuration|SHA256/)
        assert.ok(!String(error.stack).includes(key))
        return true
      })
    }
    assert.equal(externalCalls, 0)
    assert.deepEqual(calls, [])
  }
})

test('unrelated forged or misplaced signed history cannot block allocation, publication or latest', async () => {
  const foreign = completedReceipt('2026.10.500', 'b'.repeat(40))
  const badMac = clone(foreign)
  badMac.authentication.mac = 'f'.repeat(64)
  const malformed = signReceipt({ ...clone(foreign), crates: null }, signingKey)
  const malformedBytes = signReceipt({ ...clone(foreign), frontendBytes: { manifestSha256: null } }, signingKey)
  const invalidVersion = signReceipt({ ...clone(foreign), version: '1.2.3-a..b' }, signingKey)
  const wrongRepository = signReceipt({ ...clone(foreign), identity: {
    ...clone(foreign.identity), repository: 'foreign/repository' } }, signingKey)
  const examples = [release(badMac), release(malformed), release(malformedBytes), release(invalidVersion), release(wrongRepository),
    release(foreign, { tag_name: 'v2026.10.9999' }),
    release(foreign, { target_commitish: 'c'.repeat(40) }), release(foreign)]
  for (const [index, entry] of examples.entries()) {
    let tagReads = 0
    const { context, completions } = fixture({ releases: async () => [{ ...entry, id: 43, draft: false }],
      versions: async () => ['2026.10.7'],
      tagSource: async () => { tagReads++; return index === examples.length - 1 ? undefined : sourceRevision },
    })
    const result = await plan(context)
    assert.equal(result.receipt.version, '2026.10.8')
    await publish(context, result.release, result.receipt)
    assert.deepEqual(completions, [{ version: '2026.10.8', makeLatest: true }])
    assert.equal(tagReads, index === examples.length - 1 ? 3 : 0,
      'Only valid authenticated metadata may reach tag lookup; unrelated invalid tag results have no authority')
  }
})

test('history tag lookup transport failures still stop every publication boundary before crate writes', async () => {
  const foreign = completedReceipt('2026.10.500', 'b'.repeat(40))
  const failures = [new Error('GitHub request failed: HTTP 403'), new TypeError('fetch failed'),
    new assert.AssertionError({ message: 'GitHub request failed: unknown HTTP 503' })]
  for (const failure of failures) {
    const { context, calls } = fixture({ releases: async () => [release(foreign, { id: 43, draft: false })],
      tagSource: async () => { throw failure } })
    const receipt = makeReceipt()
    for (const action of [() => plan(context), () => publish(context, release(receipt), receipt),
      () => shouldMakeLatest(context, release(receipt), receipt)]) {
      await assert.rejects(action, (error) => error === failure)
    }
    assert.deepEqual(calls, [])
  }
})

test('deep untrusted JSON authentication cannot block automatic history', async () => {
  const foreign = completedReceipt('2026.10.500', 'b'.repeat(40))
  const encoded = JSON.stringify(foreign).slice(0, -1) + ',"extra":' + '['.repeat(10000) + '0' + ']'.repeat(10000) + '}'
  const entry = release(foreign, { id: 43, draft: false, target_commitish: sourceRevision,
    body: `<!-- bamboo-release-provenance\n${encoded}\n-->` })
  assert.ok(Buffer.byteLength(entry.body) > 20000 && Buffer.byteLength(entry.body) < 65536)
  assert.throws(() => readAuthenticatedReceipt(entry, signingKey), RangeError)
  const { context, completions } = fixture({ releases: async () => [entry],
    versions: async () => ['2026.10.7'],
    tagSource: async () => { throw new Error('Invalid authentication must not reach tag transport') } })
  const result = await plan(context)
  assert.equal(result.receipt.version, '2026.10.8')
  await publish(context, result.release, result.receipt)
  assert.deepEqual(completions, [{ version: '2026.10.8', makeLatest: true }])
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

test('a valid current receipt copied to a different tag cannot block or authorize automatic recovery', async () => {
  const receipt = makeReceipt()
  const canonical = release(receipt)
  const copied = { ...canonical, id: 43, tag_name: 'v2026.10.9999' }
  const originalCopy = clone(copied)
  for (const hasCanonical of [true, false]) {
    let tagReads = 0
    const { context, calls, completions } = fixture({
      releases: async () => hasCanonical ? [copied, canonical] : [copied],
      versions: async () => ['2026.10.7'],
      tagSource: async () => { tagReads++; return sourceRevision },
    })
    const result = await plan(context)
    assert.equal(result.receipt.version, receipt.version)
    assert.equal(result.release.tag_name, canonical.tag_name)
    assert.notEqual(result.release.id, copied.id)
    assert.equal(calls.includes('reserve'), !hasCanonical, 'Only canonical placement may authorize resume')
    await publish(context, result.release, result.receipt)
    assert.deepEqual(completions, [{ version: receipt.version, makeLatest: true }])
    assert.equal(tagReads, hasCanonical ? 1 : 0, 'The invalid copy must not reach tag transport at any boundary')
    assert.deepEqual(copied, originalCopy, 'The copied receipt must never be changed or re-signed')
  }
})

test('canonical authenticated history still rejects target, shape, repository and duplicate version replay', async () => {
  const receipt = makeReceipt()
  for (const changed of [release(receipt, { target_commitish: 'b'.repeat(40) }),
    release(signReceipt({ ...clone(receipt), crates: null }, signingKey)),
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

test('manual calendar guard rejects future stable versions while pure receipt decoding remains compatible', () => {
  for (const version of ['9999.1.1', '2026.11.1', '2026.9999.1',
    '18446744073709551615.1.1', '2026.18446744073709551615.1']) {
    const receipt = makeReceipt({ version, automatic: false })
    assert.equal(assertVersion(version), version)
    assert.equal(validateReceipt(receipt, identity, crates).version, version)
    assert.equal(readAuthenticatedReceipt(release(receipt), signingKey, { required: true }).version, version)
    assert.throws(() => assertManualVersion(version, now), /current UTC year\/month/)
  }
})

test('manual dispatch cannot enter the privileged receipt controller', async () => {
  const receipt = makeReceipt()
  const { context, calls } = fixture({ automatic: false,
    assertSigningKey: () => { throw new Error('Manual dispatch must not require signing credentials') } })
  await assert.rejects(() => plan(context), /only for automatic main publication/)
  await assert.rejects(() => publish(context, release(receipt), receipt), /only for automatic main publication/)
  assert.deepEqual(calls, [])
  const automatic = fixture()
  const manual = makeReceipt({ automatic: false })
  await assert.rejects(() => publish(automatic.context, release(manual), manual), /Manual history cannot authorize/)
  assert.deepEqual(automatic.calls, [])
})

test('manual stable limits follow UTC month and year boundaries while preserving historical and prerelease versions', () => {
  for (const [clock, version, allowed] of [
    ['2026-11-01T00:30:00+08:00', '2026.11.1', false],
    ['2026-10-31T17:00:00-07:00', '2026.11.1', true],
    ['2026-12-31T23:59:59.999Z', '2027.1.1', false],
    ['2027-01-01T00:00:00Z', '2027.1.1', true],
    ...['1.2.3', '1.18446744073709551615.1', '2026.10.18446744073709551615',
      '9999.1.1-rc.0'].map(version => [now.toISOString(), version, true]),
  ]) {
    if (allowed) assert.equal(assertManualVersion(version, new Date(clock)), version)
    else assert.throws(() => assertManualVersion(version, new Date(clock)), /current UTC year\/month/)
  }
})

test('manual versions obey Cargo SemVer before external operations and retain the existing build-metadata exclusion', async (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'bamboo-release-semver-'))
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }))
  fs.mkdirSync(path.join(directory, 'src'))
  fs.writeFileSync(path.join(directory, 'src/lib.rs'), '')
  const valid = ['1.2.3', '1.2.3-rc.0', '1.2.3-184467440737095516160', '18446744073709551615.1.2',
    '2026.10.18446744073709551615']
  const unsupported = ['1.2.3+001', '1.2.3-rc.0+build.001']
  const invalid = ['1.2.3-a..b', '1.2.3-.', '1.2.3-01', '1.2.3-rc.01', '1.2.3+', '1.2.3+a..b',
    '01.2.3', '1.02.3', '1.2.03', '18446744073709551616.1.2', '1.18446744073709551616.2', '1.2.18446744073709551616']
  for (const version of [...valid, ...invalid, ...unsupported]) {
    fs.writeFileSync(path.join(directory, 'Cargo.toml'), `[package]\nname="bamboo-semver-fixture"\nversion=${JSON.stringify(version)}\nedition="2021"\n`)
    const cargo = spawnSync('cargo', ['metadata', '--offline', '--no-deps', '--format-version', '1'], { cwd: directory, encoding: 'utf8' })
    if (valid.includes(version)) {
      assert.equal(assertVersion(version), version)
      assert.equal(cargo.status, 0, cargo.stderr)
    } else {
      if (unsupported.includes(version)) assert.equal(cargo.status, 0, cargo.stderr)
      else assert.notEqual(cargo.status, 0, version)
      assert.throws(() => assertManualVersion(version, now), /Pass a real, explicit publish version/)
    }
  }
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

test('trusted manual publication and automatic receipts share a queue while dry runs stay credential-free', () => {
  const workflow = fs.readFileSync('.github/workflows/publish-crate.yml', 'utf8')
  const manual = workflow.split('  manual:\n')[1].split('  dry-run:\n')[0]
  const dry = workflow.split('  dry-run:\n')[1].split('  automatic:\n')[0]
  const automatic = workflow.split('  automatic:\n')[1]
  assert.match(workflow, /workflow_run:\n    workflows: \[CI\]\n    types: \[completed\]\n    branches: \[main\]/)
  assert.match(workflow, /group: bamboo-crate-publication\n  queue: max\n  cancel-in-progress: false/)
  assert.match(workflow, /permissions:\n  contents: read/)
  assert.match(manual, /if: github.event_name == 'workflow_dispatch' && !inputs\.dry_run/)
  assert.match(manual, /environment: bamboo-release/)
  assert.match(manual, /GH_TOKEN: \$\{\{ github\.token \}\}/)
  assert.ok(manual.indexOf('Authorize the trusted manual') < manual.indexOf('uses: actions/checkout'))
  assert.ok(manual.indexOf('Require the exact accepted') < manual.indexOf('node scripts/'))
  assert.match(manual, /expected_source_sha/)
  assert.doesNotMatch(manual, /contents: write|secrets\.BAMBOO_RELEASE_|crate-release.cjs (?:plan|publish)/)
  assert.equal(manual.split('CARGO_REGISTRY_TOKEN: ${{ secrets.CARGO_REGISTRY_TOKEN }}').length - 1, 1)
  assert.match(manual, /if: github.event.inputs.dry_run != 'true'/)
  assert.match(manual, /cargo publish --locked --allow-dirty -p/)
  assert.match(dry, /if: github.event_name == 'workflow_dispatch' && inputs\.dry_run/)
  assert.doesNotMatch(dry, /environment:|secrets\.|cargo publish|crate-release.cjs (?:plan|publish)/)
  for (const name of ['GH_TOKEN', 'CARGO_REGISTRY_TOKEN', 'BAMBOO_RELEASE_TOKEN',
    'BAMBOO_RELEASE_SIGNING_KEY', 'BAMBOO_RELEASE_SIGNING_KEY_SHA256']) {
    assert.ok(dry.includes(`${name}: ""`), `${name} must be explicitly empty in dry runs`)
  }
  const aliases = [...dry.matchAll(/^      - \*([a-z-]+)$/gm)].map(match => match[1])
  assert.deepEqual(aliases, ['manual-checkout', 'manual-source', 'manual-node', 'manual-frontend',
    'manual-stage', 'manual-isolation', 'manual-version', 'manual-stamp', 'manual-lock'])
  for (const alias of aliases) {
    const step = workflow.match(new RegExp(`      - &${alias}\\n([\\s\\S]*?)(?=\\n      - |\\n  [a-z-]+:|$)`))?.[1]
    assert.ok(step, `Missing shared preparation step ${alias}`)
    assert.doesNotMatch(step, /secrets\.|BAMBOO_RELEASE_|CARGO_REGISTRY_TOKEN|cargo publish/)
  }
  assert.match(automatic, /github.event_name == 'workflow_run'/)
  assert.match(automatic, /environment: bamboo-release\n    permissions:\n      contents: write/)
  assert.match(automatic, /ref: \$\{\{ steps.source.outputs.revision \}\}\n          fetch-depth: 0\n          persist-credentials: false/)
  assert.ok(automatic.indexOf('Authorize the exact source') < automatic.indexOf('uses: actions/checkout'))
  assert.ok(automatic.indexOf('Verify checkout matches') < automatic.indexOf('node scripts/'))
  assert.doesNotMatch(automatic, /expected_source_sha|inputs\.dry_run|inputs\.version/)
  for (const name of ['BAMBOO_RELEASE_TOKEN', 'BAMBOO_RELEASE_SIGNING_KEY']) {
    assert.equal(automatic.split(`secrets.${name}`).length - 1, 2)
  }
  assert.doesNotMatch(workflow, /VERSION="\$\{\{ github.event.inputs.version/)
  const ci = fs.readFileSync('.github/workflows/ci.yml', 'utf8')
  assert.match(ci, /group: ci-\$\{\{ github.event_name \}\}/)
  assert.match(ci, /queue: \$\{\{ github.event_name == 'push' && github.ref == 'refs\/heads\/main' && 'max' \|\| 'single' \}\}/)
  assert.match(ci, /cancel-in-progress: \$\{\{ github.event_name != 'push' \|\| github.ref != 'refs\/heads\/main' \}\}/)
  assert.match(ci, /node --test scripts\/ci-policy.test.cjs scripts\/crate-release.test.cjs/)
})

test('the real manual version resolver has no signing or API dependency and never emits invalid output', (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'bamboo-manual-version-'))
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }))
  const workflow = fs.readFileSync('.github/workflows/publish-crate.yml', 'utf8')
  const inline = workflow.split("          node - <<'JS'\n")[1].split('\n          JS')[0]
    .split('\n').map(line => line.replace(/^ {10}/, '')).join('\n')
    .replace("'./scripts/crate-release.cjs'", JSON.stringify(path.join(__dirname, 'crate-release.cjs')))
  const clock = "const RealDate = Date; global.Date = class extends RealDate { constructor(...args) { super(...(args.length ? args : ['2026-10-07T12:00:00Z'])) } };\n"
  const output = path.join(directory, 'output')
  const check = (requested, sourceVersion = '0.0.0') => {
    fs.writeFileSync(path.join(directory, 'Cargo.toml'), `[package]\nversion = "${sourceVersion}"\n`)
    fs.writeFileSync(output, '')
    return spawnSync(process.execPath, ['-e', clock + inline], { cwd: directory, encoding: 'utf8',
      env: { PUBLISH_VERSION: requested, GITHUB_OUTPUT: output } })
  }
  for (const version of ['1.2.3', '2026.10.18446744073709551615', '9999.1.1-rc.0']) {
    const result = check(version)
    assert.equal(result.status, 0, result.stderr)
    assert.equal(fs.readFileSync(output, 'utf8'), `version=${version}\n`)
  }
  for (const requested of ['', 'latest']) {
    assert.equal(check(requested, '1.2.3').status, 0)
    assert.equal(fs.readFileSync(output, 'utf8'), 'version=1.2.3\n')
    assert.notEqual(check(requested).status, 0)
    assert.equal(fs.readFileSync(output, 'utf8'), '')
  }
  for (const version of ['0.0.0', '9999.1.1', '2026.11.1', '1.2.3\npoison=true', '1.2.3;false']) {
    assert.notEqual(check(version).status, 0)
    assert.equal(fs.readFileSync(output, 'utf8'), '')
  }
})

function workflowPython(name, filename = '.github/workflows/publish-crate.yml') {
  return fs.readFileSync(filename, 'utf8').split(`      - name: ${name}`)[1]
    .split("          python3 - <<'PY'\n")[1].split('\n          PY')[0]
    .split('\n').map((line) => line.replace(/^ {10}/, '')).join('\n')
}

test('Environment readiness checks authentication without executing source or uploading, and rejects invalid authority', (t) => {
  const filename = '.github/workflows/release-credentials-check.yml'
  const workflow = fs.readFileSync(filename, 'utf8')
  assert.match(workflow, /workflow_dispatch:/)
  assert.doesNotMatch(workflow, /workflow_run:|schedule:|uses:|cargo publish|actions\/checkout/)
  assert.match(workflow, /permissions:\n  contents: read/)
  assert.match(workflow, /environment: bamboo-release/)
  assert.match(workflow, /group: bamboo-crate-publication\n  queue: max\n  cancel-in-progress: false/)
  assert.match(workflow, /github\.ref_protected/)
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'bamboo-credential-check-'))
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }))
  const calls = path.join(directory, 'calls.json')
  const bootstrap = workflowPython('Check Environment authentication without publishing', filename)
  const runner = `import io, json, os, pathlib, urllib.error, urllib.request
from unittest.mock import patch
calls = []
class Opener:
    def open(self, request, timeout):
        assert request.get_method() == 'GET' and request.data is None
        assert request.get_header('Cookie') is None and timeout == 30
        url = request.full_url
        calls.append(url)
        if url == 'https://api.github.com/repos/bigduu/Bamboo-agent':
            assert request.get_header('Authorization') == 'Bearer fixture-github-secret'
            status = int(os.environ.get('FIXTURE_GITHUB_STATUS', '200'))
            if status != 200: raise urllib.error.HTTPError(url, status, '', {}, io.BytesIO(b'{}'))
            return io.BytesIO(json.dumps({'full_name': 'bigduu/Bamboo-agent', 'permissions': {'push': os.environ.get('FIXTURE_PUSH', 'true') == 'true'}}).encode())
        assert url == 'https://crates.io/api/v1/me'
        assert request.get_header('Authorization') == 'fixture-cargo-secret'
        status = int(os.environ.get('FIXTURE_CARGO_STATUS', '403'))
        if status == 200: return io.BytesIO(b'{}')
        body = json.dumps({'errors': [{'detail': os.environ.get('FIXTURE_CARGO_DETAIL', 'this action can only be performed on the crates.io website')}]}).encode()
        raise urllib.error.HTTPError(url, status, '', {}, io.BytesIO(body))
def opener(*handlers):
    assert len(handlers) == 1
    assert handlers[0].redirect_request(None, None, 302, None, None, 'https://untrusted.invalid') is None
    return Opener()
try:
    with patch('urllib.request.build_opener', opener):
        exec(compile(os.environ['FIXTURE_SCRIPT'], '<credential-check>', 'exec'))
finally:
    pathlib.Path(os.environ['FIXTURE_CALLS']).write_text(json.dumps(calls))
`
  const key = '12'.repeat(32)
  const environment = {
    PATH: process.env.PATH, GITHUB_REPOSITORY: 'bigduu/Bamboo-agent', GITHUB_EVENT_NAME: 'workflow_dispatch',
    GITHUB_REF: 'refs/heads/dev', GITHUB_REF_PROTECTED: 'true', GITHUB_SHA: sourceRevision,
    GITHUB_WORKFLOW_SHA: sourceRevision,
    GITHUB_WORKFLOW_REF: 'bigduu/Bamboo-agent/.github/workflows/release-credentials-check.yml@refs/heads/dev',
    GH_TOKEN: 'fixture-github-secret', CARGO_REGISTRY_TOKEN: 'fixture-cargo-secret',
    BAMBOO_RELEASE_SIGNING_KEY: key, BAMBOO_RELEASE_SIGNING_KEY_SHA256: sha256(Buffer.from(key, 'hex')),
    FIXTURE_SCRIPT: bootstrap, FIXTURE_CALLS: calls,
  }
  const run = (overrides = {}) => {
    fs.writeFileSync(calls, '[]')
    const result = spawnSync('python3', ['-c', runner], { encoding: 'utf8', env: { ...environment, ...overrides } })
    for (const secret of [key, environment.GH_TOKEN, environment.CARGO_REGISTRY_TOKEN]) {
      assert.ok(!`${result.stdout}${result.stderr}`.includes(secret), 'Credentials must never appear in diagnostic output')
    }
    return { ...result, calls: JSON.parse(fs.readFileSync(calls, 'utf8')) }
  }
  const accepted = run()
  assert.equal(accepted.status, 0, accepted.stderr)
  assert.equal(accepted.calls.length, 2)
  assert.match(accepted.stdout, /complete crate upload permissions still require the actual authorized release/)
  for (const overrides of [
    { GITHUB_REPOSITORY: 'foreign/repository' }, { GITHUB_EVENT_NAME: 'workflow_run' },
    { GITHUB_REF: 'refs/tags/example' }, { GITHUB_REF_PROTECTED: 'false' },
    { GITHUB_WORKFLOW_SHA: 'b'.repeat(40) }, { GITHUB_WORKFLOW_REF: 'untrusted-workflow' },
    { BAMBOO_RELEASE_SIGNING_KEY: '' }, { BAMBOO_RELEASE_SIGNING_KEY_SHA256: '0'.repeat(64) },
    { GH_TOKEN: '' }, { CARGO_REGISTRY_TOKEN: '' },
  ]) {
    const denied = run(overrides)
    assert.notEqual(denied.status, 0, JSON.stringify(overrides))
    assert.deepEqual(denied.calls, [])
  }
  for (const overrides of [
    { FIXTURE_GITHUB_STATUS: '401' }, { FIXTURE_GITHUB_STATUS: '302' }, { FIXTURE_PUSH: 'false' },
    { FIXTURE_CARGO_DETAIL: 'authentication failed' }, { FIXTURE_CARGO_DETAIL: 'insufficient scope' },
    { FIXTURE_CARGO_STATUS: '500' }, { FIXTURE_CARGO_STATUS: '200' },
  ]) assert.notEqual(run(overrides).status, 0, 'Unavailable or unverified authentication must fail closed')
})

test('manual inline bootstrap accepts only protected branches and exact annotated trusted source tags before checkout', (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'bamboo-manual-bootstrap-'))
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }))
  const git = (...args) => {
    const result = spawnSync('git', ['-c', 'user.name=Manual fixture', '-c', 'user.email=fixture@example.invalid', ...args],
      { cwd: directory, encoding: 'utf8' })
    assert.equal(result.status, 0, result.stderr)
    return result.stdout.trim()
  }
  git('init', '-q')
  const commit = (value) => {
    fs.writeFileSync(path.join(directory, 'source'), value)
    git('add', 'source')
    git('commit', '-qm', value)
    return git('rev-parse', 'HEAD')
  }
  const historical = commit('historical')
  const main = commit('main')
  const dev = commit('dev')
  git('checkout', '--detach', historical)
  const feature = commit('feature')
  const tagName = source => `bamboo-release-source-${source}`
  const tagRef = source => `refs/tags/${tagName(source)}`
  git('tag', '-a', tagName(historical), historical, '-m', 'Accepted source')
  git('tag', '-a', tagName(feature), feature, '-m', 'Unaccepted source')
  const bin = path.join(directory, 'bin')
  fs.mkdirSync(bin)
  const calls = path.join(directory, 'api-calls')
  fs.writeFileSync(path.join(bin, 'gh'), `#!/usr/bin/env python3
import base64, json, os, pathlib, subprocess, sys
route = sys.argv[2]
with open(os.environ['FIXTURE_CALLS'], 'a') as stream: stream.write(route + '\\n')
if os.environ.get('FIXTURE_API_FAILURE') == 'true': sys.exit(1)
responses = json.loads(os.environ.get('FIXTURE_RESPONSES', '{}'))
if route in responses:
    print(json.dumps(responses[route]))
    sys.exit(0)
def git(*args):
    return subprocess.check_output(['git', *args], cwd=os.environ['FIXTURE_REPOSITORY'], text=True).strip()
if '/branches/' in route:
    name = route.rsplit('/', 1)[-1]
    result = {'name': name, 'protected': os.environ.get('FIXTURE_UNPROTECTED') != 'true', 'commit': {'sha': json.loads(os.environ['FIXTURE_TIPS'])[name]}}
elif '/compare/' in route:
    source, tip = route.rsplit('/', 1)[-1].split('...')
    base = git('merge-base', source, tip)
    status = 'identical' if source == tip else 'ahead' if base == source else 'behind' if base == tip else 'diverged'
    result = {'status': status, 'merge_base_commit': {'sha': base}}
elif '/git/ref/tags/' in route:
    ref = 'refs/tags/' + route.split('/git/ref/tags/')[1]
    sha = git('rev-parse', ref)
    result = {'ref': ref, 'object': {'type': git('cat-file', '-t', sha), 'sha': sha}}
elif '/git/tags/' in route:
    sha = route.rsplit('/', 1)[-1]
    headers = dict(line.split(' ', 1) for line in git('cat-file', '-p', sha).split('\\n\\n')[0].splitlines())
    result = {'sha': sha, 'tag': headers['tag'], 'object': {'type': headers['type'], 'sha': headers['object']}}
elif '/contents/' in route:
    content = pathlib.Path(os.environ['FIXTURE_POLICY']).read_bytes()
    if os.environ.get('FIXTURE_OLD_POLICY') == 'true': content = content.replace(b'# BAMBOO_TRUSTED_PUBLICATION_POLICY_V1', b'# old policy')
    result = {'type': 'file', 'path': '.github/workflows/publish-crate.yml', 'encoding': 'base64', 'content': base64.b64encode(content).decode()}
else: raise AssertionError('Unexpected route: ' + route)
print(json.dumps(result))
`, { mode: 0o755 })
  const bootstrap = workflowPython('Authorize the trusted manual publication ref before checkout')
  const run = (source = historical, ref = tagRef(source), overrides = {}) => {
    fs.writeFileSync(calls, '')
    const result = spawnSync('python3', ['-c', bootstrap], { cwd: directory, encoding: 'utf8', env: {
      PATH: `${bin}:${process.env.PATH}`, GH_TOKEN: 'dummy-readonly',
      GITHUB_REPOSITORY: 'bigduu/Bamboo-agent', GITHUB_EVENT_NAME: 'workflow_dispatch',
      EXPECTED_SOURCE_SHA: source, GITHUB_SHA: source, GITHUB_WORKFLOW_SHA: source,
      GITHUB_REF: ref, GITHUB_REF_TYPE: ref.startsWith('refs/tags/') ? 'tag' : 'branch', GITHUB_REF_PROTECTED: 'true',
      GITHUB_WORKFLOW_REF: `bigduu/Bamboo-agent/.github/workflows/publish-crate.yml@${ref}`,
      FIXTURE_REPOSITORY: directory, FIXTURE_CALLS: calls, FIXTURE_TIPS: JSON.stringify({ dev, main }),
      FIXTURE_POLICY: path.resolve('.github/workflows/publish-crate.yml'), ...overrides,
    } })
    return { ...result, calls: fs.readFileSync(calls, 'utf8') }
  }
  for (const [source, ref] of [[historical, tagRef(historical)], [dev, 'refs/heads/dev'],
    [main, 'refs/heads/main'], [historical, 'refs/heads/dev']]) {
    const result = run(source, ref)
    assert.equal(result.status, 0, result.stderr)
    assert.match(result.calls, new RegExp(`contents/.github/workflows/publish-crate.yml\\?ref=${source}`))
  }
  for (const [ref, overrides] of [
    ['refs/heads/feature', {}], ['refs/tags/bamboo-bodhi-source-2026.10.9', {}],
    [tagRef(dev), {}], [tagRef(historical) + '-extra', {}],
    [tagRef(historical), { GITHUB_REF_TYPE: 'branch' }],
    [tagRef(historical), { GITHUB_REF_PROTECTED: 'false' }],
    [tagRef(historical), { GITHUB_WORKFLOW_SHA: dev }],
    [tagRef(historical), { GITHUB_SHA: dev }],
    [tagRef(historical), { EXPECTED_SOURCE_SHA: 'not-a-sha' }],
    [tagRef(historical), { GITHUB_REPOSITORY: 'foreign/repository' }],
    [tagRef(historical), { GITHUB_EVENT_NAME: 'workflow_run' }],
    [tagRef(historical), { GITHUB_WORKFLOW_REF: 'bigduu/Bamboo-agent/.github/workflows/other.yml@' + tagRef(historical) }],
  ]) {
    const result = run(historical, ref, overrides)
    assert.notEqual(result.status, 0, `${ref} must not authorize checkout`)
    assert.equal(result.calls, '', 'Untrusted dispatch must fail before API or repository execution')
  }
  for (const overrides of [{ FIXTURE_API_FAILURE: 'true' }, { FIXTURE_UNPROTECTED: 'true' }, { FIXTURE_OLD_POLICY: 'true' }]) {
    const result = run(historical, tagRef(historical), overrides)
    assert.notEqual(result.status, 0, 'Unavailable API, unprotected history or pre-policy source must fail closed')
  }
  assert.notEqual(run(feature).status, 0, 'A protected tag cannot admit unrelated source history')
  const annotation = git('rev-parse', tagRef(historical))
  const tagRoute = `repos/bigduu/Bamboo-agent/git/tags/${annotation}`
  for (const response of [
    { sha: annotation, tag: tagName(historical), object: { type: 'commit', sha: dev } },
    { sha: annotation, tag: tagName(historical), object: { type: 'tag', sha: annotation } },
    { sha: annotation, tag: 'another-tag', object: { type: 'commit', sha: historical } },
    { sha: dev, tag: tagName(historical), object: { type: 'commit', sha: historical } },
  ]) {
    const result = run(historical, tagRef(historical), { FIXTURE_RESPONSES: JSON.stringify({ [tagRoute]: response }) })
    assert.notEqual(result.status, 0, 'Malformed, moved and nested annotation identities cannot authorize checkout')
    assert.doesNotMatch(result.calls, /contents\//)
  }
  git('tag', '-d', tagName(historical))
  git('tag', tagName(historical), historical)
  assert.notEqual(run().status, 0, 'A real lightweight tag must fail')
  git('tag', '-fa', tagName(historical), dev, '-m', 'Moved source')
  assert.notEqual(run().status, 0, 'A real moved annotated tag must fail')
})

test('automatic inline bootstrap authorizes the exact successful CI source and protected historical workflow before checkout', (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'bamboo-release-bootstrap-'))
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }))
  const git = (...args) => {
    const result = spawnSync('git', args, { cwd: directory, encoding: 'utf8' })
    assert.equal(result.status, 0, result.stderr)
    return result.stdout.trim()
  }
  git('init', '-q')
  const commit = (value) => {
    fs.writeFileSync(path.join(directory, 'source'), value)
    git('add', 'source')
    git('-c', 'user.name=Bootstrap fixture', '-c', 'user.email=fixture@example.invalid', 'commit', '-qm', value)
    return git('rev-parse', 'HEAD')
  }
  const historical = commit('historical')
  const main = commit('main')
  const dev = commit('dev')
  git('checkout', '--detach', historical)
  const feature = commit('feature')
  const bin = path.join(directory, 'bin')
  fs.mkdirSync(bin)
  const calls = path.join(directory, 'api-calls')
  fs.writeFileSync(path.join(bin, 'gh'), `#!/usr/bin/env python3
import json, os, pathlib, subprocess, sys
route = sys.argv[2]
with open(os.environ['FIXTURE_CALLS'], 'a') as stream: stream.write(route + '\\n')
if os.environ.get('FIXTURE_API_FAILURE') == 'true': sys.exit(1)
if '/branches/' in route:
    name = route.rsplit('/', 1)[-1]
    print(json.dumps({'name': name, 'protected': os.environ.get('FIXTURE_UNPROTECTED') != 'true', 'commit': {'sha': json.loads(os.environ['FIXTURE_TIPS'])[name]}}))
else:
    source, tip = route.rsplit('/', 1)[-1].split('...')
    base = subprocess.check_output(['git', 'merge-base', source, tip], cwd=os.environ['FIXTURE_REPOSITORY'], text=True).strip()
    status = 'identical' if source == tip else 'ahead' if base == source else 'behind' if base == tip else 'diverged'
    print(json.dumps({'status': status, 'merge_base_commit': {'sha': base}}))
`, { mode: 0o755 })
  const eventPath = path.join(directory, 'event.json')
  const output = path.join(directory, 'output')
  const bootstrap = workflowPython('Authorize the exact source before checkout')
  const event = { workflow_run: { name: 'CI', event: 'push', conclusion: 'success', head_branch: 'main', head_sha: historical,
    repository: { full_name: 'bigduu/Bamboo-agent' }, head_repository: { full_name: 'bigduu/Bamboo-agent' } } }
  const run = (overrides = {}, payload = event) => {
    fs.writeFileSync(eventPath, JSON.stringify(payload))
    fs.writeFileSync(output, '')
    fs.writeFileSync(calls, '')
    const result = spawnSync('python3', ['-c', bootstrap], { encoding: 'utf8', env: { ...process.env,
      PATH: `${bin}:${process.env.PATH}`, GITHUB_REPOSITORY: 'bigduu/Bamboo-agent', GITHUB_EVENT_NAME: 'workflow_run',
      GITHUB_EVENT_PATH: eventPath, GITHUB_SHA: historical, GITHUB_OUTPUT: output, GITHUB_REF: 'refs/heads/dev',
      GITHUB_REF_PROTECTED: 'true', GITHUB_WORKFLOW_REF: 'bigduu/Bamboo-agent/.github/workflows/publish-crate.yml@refs/heads/dev',
      GITHUB_WORKFLOW_SHA: dev, FIXTURE_REPOSITORY: directory, FIXTURE_CALLS: calls,
      FIXTURE_TIPS: JSON.stringify({ dev, main }), ...overrides } })
    return { ...result, output: fs.readFileSync(output, 'utf8'), calls: fs.readFileSync(calls, 'utf8') }
  }
  for (const source of [historical, dev, main]) {
    const payload = clone(event)
    payload.workflow_run.head_sha = source
    const result = run({}, payload)
    assert.equal(result.status, 0, result.stderr)
    assert.equal(result.output, `revision=${source}\n`)
  }
  for (const invalid of [feature, 'f'.repeat(40), 'invalid']) {
    const payload = clone(event)
    payload.workflow_run.head_sha = invalid
    const result = run({}, payload)
    assert.notEqual(result.status, 0)
    assert.equal(result.output, '')
  }
  for (const overrides of [{ GITHUB_WORKFLOW_SHA: feature }, { FIXTURE_UNPROTECTED: 'true' }, { FIXTURE_API_FAILURE: 'true' },
    { GITHUB_REF: 'refs/heads/feature' }, { GITHUB_REF_PROTECTED: 'false' },
    { GITHUB_WORKFLOW_REF: 'bigduu/Bamboo-agent/.github/workflows/publish-crate.yml@refs/heads/feature' }]) {
    const result = run(overrides)
    assert.notEqual(result.status, 0)
    assert.equal(result.output, '', 'Untrusted source cannot be handed to checkout or repository code')
  }
  const automatic = run({ GITHUB_SHA: dev })
  assert.equal(automatic.status, 0, automatic.stderr)
  assert.equal(automatic.output, `revision=${historical}\n`, 'workflow SHA cannot replace the tested CI source')
  for (const [key, value] of [['name', 'Other'], ['head_repository', { full_name: 'foreign/repository' }],
    ['event', 'pull_request'], ['conclusion', 'failure'], ['head_branch', 'dev']]) {
    const changed = clone(event)
    changed.workflow_run[key] = value
    assert.notEqual(run({}, changed).status, 0)
  }
  const manual = run({ GITHUB_EVENT_NAME: 'workflow_dispatch', GITHUB_REF: 'refs/tags/frozen-source' })
  assert.notEqual(manual.status, 0)
  assert.equal(manual.output, '')
  assert.equal(manual.calls, '')

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
  const python = ['python3', 'python3.12', 'python3.14'].find((candidate) =>
    spawnSync(candidate, ['-c', 'import tomllib']).status === 0)
  assert.ok(python, 'Python >= 3.11 is required by the publication manifest policy')
  const result = spawnSync(python, [path.join(__dirname, 'crate-release-lock.py'), 'stamp', '2026.10.8'],
    { cwd: directory, encoding: 'utf8' })
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
