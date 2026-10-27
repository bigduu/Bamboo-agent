// Registered by sidecar-release.test.cjs, the existing automatic policy owner.
const assert = require('node:assert/strict')
const { test } = require('node:test')
const core = require('./crate-release.cjs')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { spawnSync } = require('node:child_process')
const sidecar = require('./sidecar-release.cjs')
const original = require('./native-originals.cjs')
const copy = value => JSON.parse(JSON.stringify(value))
const key = 'a'.repeat(64) // Synthetic test authority, never a host credential.
const timestamp = second => `2026-10-27T00:00:${String(second).padStart(2, '0')}Z`

function job(identity, target, id, attempt = 1, names = [original.SELECT, original.BUILD, original.UPLOAD, original.RECORD]) {
  return { id, run_id: Number(identity.runId), run_attempt: attempt, head_sha: identity.workflowRevision,
    head_branch: 'main', name: original.jobName(target), status: 'completed', conclusion: 'success',
    started_at: timestamp(0), completed_at: timestamp(20),
    steps: names.map((name, index) => ({ number: index + 1, name, status: 'completed', conclusion: 'success',
      started_at: timestamp(index + 1), completed_at: timestamp(index + 2) })) }
}

function fixture(t) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'bamboo-native-originals-'))
  t.after(() => fs.rmSync(root, { recursive: true, force: true }))
  const identity = { repository: 'bigduu/Bamboo-agent', sourceRevision: '1'.repeat(40), sourceTree: '2'.repeat(40),
    workflowRevision: '3'.repeat(40), runId: '100', runAttempt: 1, version: '2026.10.123', ciRun: 'fixture/main/90' }
  const frontendBytes = { manifestSha256: '4'.repeat(64), archiveSha256: '5'.repeat(64) }
  const receipt = core.signReceipt({ schemaVersion: 1, version: identity.version,
    identity: { repository: identity.repository, sourceRevision: identity.sourceRevision, frontend: {} },
    crates: ['bamboo-agent'], frontendBytes, automatic: true, completed: false, ciRun: identity.ciRun,
    packageChecksums: {}, nativeOriginals: core.nativeOriginalContract(identity) }, key)
  const release = { id: 42, tag_name: `v${identity.version}`, target_commitish: identity.sourceRevision,
    draft: true, prerelease: false, body: core.receiptBody(receipt) }
  const preparation = core.createPreparation({ release, receipt }, identity, 2)
  const assets = new Map(), artifacts = new Map(), logs = new Map(), rows = [], uploads = []
  let sequence = 2000, files = 0, hostBuilds = 0, guards = 0
  const file = name => path.join(root, `${++files}-${name}`)
  const context = { identity, preparation, signingKey: key, file,
    directory: (target, phase) => path.join(root, `${++files}-${phase}-${target}`),
    guard: async () => { guards++ }, readRelease: async () => copy(release),
    readTag: async () => ({ object: { type: 'commit', sha: identity.sourceRevision } }),
    readRun: async () => ({ id: Number(identity.runId), name: 'Publish Crate', path: '.github/workflows/publish-crate.yml',
      head_sha: identity.workflowRevision, run_attempt: identity.runAttempt, status: 'in_progress', event: 'workflow_run',
      repository: { full_name: identity.repository }, head_repository: { full_name: identity.repository } }),
    listAssets: async () => [...assets.values()].map(value => copy(value.metadata)),
    readAsset: async asset => {
      const value = [...assets.values()].find(entry => entry.metadata.id === asset.id)
      assert.ok(value, 'Missing asset fixture')
      const destination = file(asset.name); fs.writeFileSync(destination, value.bytes); return destination
    },
    upload: async (source, name) => {
      assert.ok(!assets.has(name), 'Fixture refuses replacement')
      const bytes = fs.readFileSync(source)
      assets.set(name, { bytes, metadata: { id: ++sequence, name, state: 'uploaded', size: bytes.length,
        digest: `sha256:${core.sha256(bytes)}` } }); uploads.push(name)
    },
    jobs: async () => copy(rows), readJob: async id => copy(rows.find(value => value.id === id)),
    readLog: async id => logs.get(id),
    readArtifactMetadata: async id => copy(artifacts.get(id).metadata),
    readArtifact: async metadata => artifacts.get(metadata.id).file,
    buildHost: async () => {
      hostBuilds++
      const destination = file(sidecar.HOST_ASSET), bytes = Buffer.from('synthetic host bytes')
      fs.writeFileSync(destination, bytes)
      return { file: destination, manifest: { assetName: sidecar.HOST_ASSET,
        sha256: core.sha256(bytes), sizeBytes: bytes.length,
        files: sidecar.HOST_FILES.map(name => ({ path: name, sha256: '6'.repeat(64), sizeBytes: 1 })) } }
    } }
  function add(target, { id = 300 + rows.length, attempt = 1, zipMutate } = {}) {
    const bytes = Buffer.alloc(65536), native = sidecar.artifactName(target)
    if (target.includes('apple')) { bytes.writeUInt32LE(0xfeedfacf); bytes.writeUInt32LE(0x0100000c, 4); bytes.writeUInt32LE(2, 12) }
    else if (target.includes('windows')) {
      bytes.write('MZ'); bytes.writeUInt32LE(128, 60); bytes.writeUInt32LE(0x4550, 128)
      bytes.writeUInt16LE(0x8664, 132); bytes.writeUInt16LE(2, 150); bytes.writeUInt16LE(0x20b, 152)
    } else { Buffer.from('7f454c460201', 'hex').copy(bytes); bytes.writeUInt16LE(3, 16); bytes.writeUInt16LE(62, 18) }
    const build = { schemaVersion: 1, ...identity, runAttempt: attempt, target, buildMode: 'embedded', embeddedFrontend: true,
      format: 'raw', assetName: native, sha256: core.sha256(bytes), sizeBytes: bytes.length,
      command: ['cargo', 'build', '--locked', '--release', '--verbose', '--bin', 'bamboo', '--target', target,
        '--message-format=json-render-diagnostics'], runtime: { status: 'passed', version: `bamboo ${identity.version}` },
      frontendBytes, cargoMessagesSha256: '7'.repeat(64), stampedDiffSha256: '8'.repeat(64), rustc: 'rustc fixture', cargo: 'cargo fixture' }
    const binary = file(native), receiptFile = file('build-receipt.json'), zip = file('original.zip')
    fs.writeFileSync(binary, bytes); const receiptBytes = Buffer.from(`${JSON.stringify(build)}\n`); fs.writeFileSync(receiptFile, receiptBytes)
    const script = 'import sys,zipfile; z=zipfile.ZipFile(sys.argv[3],"w",compression=zipfile.ZIP_DEFLATED); z.write(sys.argv[1],sys.argv[4]); z.write(sys.argv[2],"build-receipt.json"); z.close()'
    const built = spawnSync(process.env.PYTHON || 'python3', ['-c', script, binary, receiptFile, zip, native], {
      encoding: 'utf8', env: { PATH: process.env.PATH } })
    assert.equal(built.status, 0, built.stderr)
    if (zipMutate) zipMutate(zip)
    const info = sidecar.fileIdentity(zip), artifactId = 1000 + id
    artifacts.set(artifactId, { file: zip, metadata: { id: artifactId, name: original.transportName(target, attempt),
      workflow_run: { id: Number(identity.runId), head_sha: identity.workflowRevision },
      digest: `sha256:${info.sha256}`, size_in_bytes: info.sizeBytes, expired: false } })
    const physical = job(identity, target, id, attempt)
    rows.push(physical)
    const transport = original.transportRecord({ ...identity, runAttempt: attempt }, target, receiptBytes,
      { id: artifactId, sha256: info.sha256 })
    original.emit(original.TRANSPORT_PREFIX, transport, value => logs.set(id, `${timestamp(19)} ${value}\n`))
    return { physical, transport, build, zip, artifactId }
  }
  function prepareJob(attempt = identity.runAttempt) {
    const current = job(identity, original.TARGETS[0], 900 + attempt, attempt, [original.PREPARE])
    current.name = 'automatic'; current.status = 'in_progress'; current.conclusion = null
    current.steps[0].status = 'in_progress'; current.steps[0].conclusion = null
    rows.push(current); return current
  }
  return { root, identity, preparation, context, release, assets, artifacts, rows, logs, uploads, add, prepareJob,
    get hostBuilds() { return hostBuilds }, get guards() { return guards } }
}

function register() {
  test('Raw2358 Native selector and collector retain ANSI bytes without changing JSON or binary reads', async t => {
    const f = fixture(t), preparing = job(f.identity, original.TARGETS[0], 901, 1, [original.PREPARE])
    preparing.name = 'automatic'
    const current = job(f.identity, original.TARGETS[0], 777, 1, [original.SELECT])
    current.status = 'in_progress'; current.conclusion = null
    current.steps[0].status = 'in_progress'; current.steps[0].conclusion = null
    const value = core.signReceipt({ schemaVersion: 1, kind: original.PLAN_KIND,
      reservationSha256: original.contract(f.preparation, f.identity), runId: f.identity.runId,
      runAttempt: 1, controllerRevision: f.identity.workflowRevision, preparationJobId: preparing.id,
      targets: original.TARGETS.map(target => ({ target, action: 'build' })) }, key)
    let frame; original.emit(original.PLAN_PREFIX, value, line => { frame = line })
    const log = `${timestamp(0)} \u001b[33moriginal native output\u001b[0m\r\n${timestamp(19)} ${frame}\n`
    const binary = Buffer.from([80, 75, 0, 27, 255]), calls = [], execute = sidecar.execute, github = core.github
    try {
      sidecar.execute = (program, args, options = {}) => {
        assert.equal(program, 'gh'); calls.push({ args, options }); const route = args[1]
        if (route.endsWith(`/actions/jobs/${preparing.id}/logs`)) {
          assert.deepEqual(args, ['api', `repos/${f.identity.repository}/actions/jobs/${preparing.id}/logs`, '--allow-escape-sequences'])
          assert.equal(options.maxBuffer, 64 * 1024 * 1024); return log
        }
        assert.ok(!args.includes('--allow-escape-sequences'))
        if (route.endsWith('/attempts/1/jobs?per_page=100&page=1')) return JSON.stringify({ total_count: 2, jobs: [preparing, current] })
        if (route.endsWith(`/actions/jobs/${current.id}`)) return JSON.stringify(current)
        if (route.endsWith(`/actions/jobs/${preparing.id}`)) return JSON.stringify(preparing)
        assert.equal(route, `repos/${f.identity.repository}/actions/artifacts/9/zip`)
        assert.equal(options.encoding, null); assert.equal(options.maxBuffer, original.MAX_ZIP); return binary
      }
      core.github = (repository, route) => { assert.equal(repository, f.identity.repository); assert.equal(route, 'actions/runs/100'); return { inert: true } }
      await t.test('read-only physical worker', async () => {
        assert.equal((await original.workerSelection(f.identity, f.preparation, value, original.TARGETS[0], true)).action, 'build')
      })
      await t.test('collector raw bytes and unchanged binary port', async () => {
        const context = original.createContext(f.identity, f.preparation, { RUNNER_TEMP: f.root, GITHUB_REPOSITORY: f.identity.repository,
          GITHUB_SHA: f.identity.workflowRevision, GITHUB_WORKFLOW_SHA: f.identity.workflowRevision, GITHUB_RUN_ID: f.identity.runId,
          GITHUB_RUN_ATTEMPT: '1', BAMBOO_RELEASE_SIGNING_KEY: key, BAMBOO_RELEASE_SIGNING_KEY_SHA256: core.sha256(Buffer.from(key, 'hex')) })
        assert.deepEqual(Buffer.from(await context.readLog(preparing.id)), Buffer.from(log))
        assert.equal(context.readRun(100).inert, true)
        assert.deepEqual(fs.readFileSync(context.readArtifact({ id: 9 })), binary)
      })
      assert.ok(calls.length >= 2)
    } finally { sidecar.execute = execute; core.github = github }
  })

  test('2344 Source selector authenticates the whole preparation file through the original physical plan log', async t => {
    const f = fixture(t), facts = { schemaVersion: 2, kind: 'bamboo-frozen-source-context/v2', publicationAuthorized: false,
      source: { revision: f.identity.sourceRevision, tree: f.identity.sourceTree, acceptedMerge: f.identity.sourceRevision,
        releaseRef: `release/${f.identity.sourceRevision}`, mainHistoryAnchor: '4'.repeat(40) },
      controller: { revision: f.identity.workflowRevision, workflowPath: '.github/workflows/publish-crate.yml', event: 'workflow_dispatch', ref: 'refs/heads/main' },
      feature: { pr: 99, runId: 90, logicalAttempt: 1, workflowId: 8, checkSuiteId: 9, producerDigest: '5'.repeat(64),
        baseRevision: '6'.repeat(40), headRevision: '7'.repeat(40), snapshotRevision: '8'.repeat(40), workflowDefinitionRevision: '7'.repeat(40),
        mergedAt: timestamp(30), physicalJobs: [] }, inputs: { fixtureDataOnly: true } }
    f.identity.sourceDispatch = core.sourceDispatchContract(facts, f.identity.runId)
    f.identity.ciRun = 'https://github.com/bigduu/Bamboo-agent/actions/runs/90'
    const receipt = copy(f.preparation.receipt); Object.assign(receipt, { ciRun: f.identity.ciRun, sourceDispatch: f.identity.sourceDispatch,
      frontendOriginals: { schemaVersion: 1, complete: true, assets: [
        { assetId: 1, name: 'frontend-manifest.json', sizeBytes: 1, sha256: receipt.frontendBytes.manifestSha256 },
        { assetId: 2, name: 'lotus-frontend.zip', sizeBytes: 1, sha256: receipt.frontendBytes.archiveSha256 }] } })
    core.signReceipt(receipt, key); f.release.body = core.receiptBody(receipt)
    Object.assign(f.preparation, core.createPreparation({ release: f.release, receipt }, f.identity, 2))
    const readRun = f.context.readRun; f.context.readRun = async () => ({ ...await readRun(), event: 'workflow_dispatch' })
    const preparing = f.prepareJob(), plan = await original.plan(f.context, preparing.id)
    assert.equal(plan.preparationSha256, core.sha256(Buffer.from(core.canonicalJson(f.preparation))))
    preparing.status = 'completed'; preparing.conclusion = 'success'
    preparing.steps[0].status = 'completed'; preparing.steps[0].conclusion = 'success'
    f.logs.set(preparing.id, timestamp(20) + ' ' + original.PLAN_PREFIX + Buffer.from(JSON.stringify(plan)).toString('base64'))
    const current = job(f.identity, original.TARGETS[0], 555, 1, [original.SELECT])
    current.status = 'in_progress'; current.conclusion = null; current.steps[0].status = 'in_progress'; current.steps[0].conclusion = null
    f.rows.push(current)
    assert.equal((await original.selectCurrent(f.context, plan, original.TARGETS[0], current)).action, 'build')
    const changed = copy(f.preparation); changed.receipt.packageChecksums['bamboo-agent'] = '9'.repeat(64)
    assert.throws(() => original.select(plan, f.identity, changed, original.TARGETS[0], current), /original preparation file/)
    await assert.rejects(original.selectCurrent(f.context, { ...plan, preparationSha256: '0'.repeat(64) }, original.TARGETS[0], current), /original preparation file/)
  })

  test('2344 whole-release job proof keeps physical projections and requires every latest real success', async t => {
    const identity = { runId: '100', runAttempt: 2, workflowRevision: '3'.repeat(40) }
    const physical = Object.entries(original.WHOLE_STEPS).map(([name, steps], index) =>
      ({ ...job(identity, original.TARGETS[0], 600 + index, 1, steps), name }))
    const latest = physical.map(value => ({ ...copy(value), id: value.id + 100, run_attempt: 2 }))
    const context = { identity, latestJobs: async () => copy(latest), jobs: async () => copy([...physical, ...latest]),
      readJob: async id => copy([...physical, ...latest].find(value => value.id === id)) }
    const proof = await original.wholeJobs(context)
    assert.deepEqual(proof.map(value => value.physical.id), physical.map(value => value.id))
    assert.deepEqual(proof.map(value => value.logical.id), latest.map(value => value.id))
    assert.equal(proof[0].stepsSha256, core.sha256(Buffer.from(original.executionKey(physical[0]))))
    for (const [name, mutate] of [
      ['latest failure', (rows, all) => { rows[1].conclusion = 'failure'; all[physical.length + 1].conclusion = 'failure' }],
      ['skipped critical step', (rows, all) => { rows[1].steps[0].conclusion = 'skipped'; all[physical.length + 1].steps[0].conclusion = 'skipped' }],
      ['missing owner', rows => rows.pop()],
      ['same-attempt ambiguity', (_rows, all) => all.push({ ...copy(physical[0]), id: 999 })],
      ['changed direct physical', (_rows, _all, direct) => { direct.steps[0].name += ' changed' }],
    ]) await t.test(name, async () => {
      const rows = copy(latest), all = copy([...physical, ...latest]), direct = copy(physical[0]); mutate(rows, all, direct)
      await assert.rejects(original.wholeJobs({ identity, latestJobs: async () => rows, jobs: async () => all,
        readJob: async id => id === direct.id ? direct : all.find(value => value.id === id) }))
    })
  })

  test('2344 whole completion binds the exact public closure and preserves an earlier failed crate origin', () => {
    const identity = { repository: sidecar.REPOSITORY, sourceRevision: '1'.repeat(40), sourceTree: '2'.repeat(40),
      workflowRevision: '3'.repeat(40), runId: '100', runAttempt: 2, version: '2026.10.123' }
    const ref = (name, id = 1001) => ({ assetId: id, name, sha256: '4'.repeat(64), sizeBytes: 1 })
    const manifestBytes = Buffer.from('original manifest bytes')
    const names = [...original.TARGETS.map(sidecar.artifactName), sidecar.HOST_ASSET, sidecar.PROVENANCE, sidecar.MANIFEST]
    const assets = names.map((name, index) => ({ id: 20 + index, name, state: 'uploaded', size: 1,
      digest: `sha256:${name === sidecar.MANIFEST ? core.sha256(manifestBytes) : '4'.repeat(64)}` }))
    const receipt = { completed: true, crates: ['bamboo-agent'], frontendOriginals: { assets: [ref('frontend-manifest.json'), ref('lotus-frontend.zip', 1002)] },
      sourceDispatch: { runId: '100', context: { source: { releaseRef: `release/${identity.sourceRevision}` }, feature: { pr: 99, runId: 90 } } } }
    const finalizer = { ...job(identity, original.TARGETS[0], 900, 2, [original.WHOLE_STEP]), name: original.WHOLE_JOB,
      status: 'in_progress', conclusion: null, completed_at: null }
    Object.assign(finalizer.steps[0], { status: 'in_progress', conclusion: null, completed_at: null })
    const input = { context: { identity }, fresh: { release: { id: 42, tag_name: `v${identity.version}`,
      target_commitish: identity.sourceRevision, draft: false, prerelease: false }, receipt, assets }, manifestBytes,
      restored: { originals: original.TARGETS.map(target => ({ target, archive: ref(original.archiveName(target)), checkpoint: ref(original.checkpointName(target)) })),
        preparedHost: { checkpoint: ref(original.HOST_CHECKPOINT) } }, receiptAsset: ref('release-provenance.json'),
      crateClosure: { kind: 'bamboo-verified-crate-closure/v1', publicationAuthorized: false, crates: [{ name: 'bamboo-agent',
        archive: { sha256: '5'.repeat(64) }, asset: { id: 333, name: 'crate.zip', digest: `sha256:${'6'.repeat(64)}`, size: 20 },
        origin: { jobId: 12, attempt: 1 }, originalJob: { status: 'completed', conclusion: 'failure' },
        verification: { exitCode: 0 }, registryChecksum: '5'.repeat(64) }] },
      jobs: Object.keys(original.WHOLE_STEPS).map(name => ({ name, logical: { id: 200, attempt: 2 }, physical: { id: 100, attempt: 1 }, stepsSha256: '7'.repeat(64) })),
      run: { workflow_id: 8, check_suite_id: 9, check_suite_node_id: 'CS_fixture' }, finalizer }
    const { frame, line } = original.wholeCompletion(input)
    assert.equal(line, original.WHOLE_PREFIX + Buffer.from(core.canonicalJson(frame)).toString('base64'))
    assert.equal(frame.publicationAuthorized, false); assert.equal(frame.crates[0].origin.jobId, 12)
    assert.equal(frame.crates[0].origin.attempt, 1); assert.equal(frame.controller.finalizerJobId, finalizer.id)
    assert.deepEqual(frame.release.receiptAsset, input.receiptAsset)
    assert.equal(frame.release.receiptSha256, core.sha256(Buffer.from(core.canonicalJson(receipt))))
    assert.equal(JSON.stringify(frame).includes(key), false, 'Public data has no signing key')
    assert.throws(() => original.wholeCompletion({ ...input, fresh: { ...input.fresh, release: { ...input.fresh.release, draft: true } } }))
    assert.throws(() => original.wholeCompletion({ ...input, jobs: input.jobs.slice(1) }))
    assert.throws(() => original.wholeCompletion({ ...input, crateClosure: { ...input.crateClosure, crates: [] } }))
    assert.throws(() => original.wholeCompletion({ ...input, finalizer: { ...finalizer, status: 'completed', conclusion: 'success' } }))
  })

  test('native originals have one existing automatic feature policy owner', () => {
    const source = fs.readFileSync('.github/workflows/ci.yml', 'utf8')
    const quick = source.split('  quick-checks:\n')[1].split(/\n  [a-z][\w-]*:\n/)[0]
    assert.equal([...quick.matchAll(/run: node --test[^\n]*scripts\/sidecar-release\.test\.cjs/g)].length, 1)
    assert.doesNotMatch(source, /run: node --test[^\n]*scripts\/native-originals\.test\.cjs/)
    assert.equal(fs.readFileSync('scripts/sidecar-release.test.cjs', 'utf8').split("require('./native-originals.test.cjs').register()").length, 2)
  })

  test('native originals requires an explicit signed contract without upgrading historical preparation', () => {
    assert.equal(typeof core.nativeOriginalContract, 'function')
    const identity = { workflowRevision: '1'.repeat(40), runId: '100', runAttempt: 1 }
    const marker = core.nativeOriginalContract(identity)
    assert.equal(marker.kind, 'bamboo-native-originals/v1')
    assert.equal(marker.controllerRevision, identity.workflowRevision)
    assert.equal(marker.runId, identity.runId)
    assert.throws(() => core.nativeOriginalContract({ ...identity, runId: 'not-a-run' }))
  })

  test('native originals selector rejects copied preparation before any build prerequisite', () => {
    assert.equal(typeof original.select, 'function')
    let effects = 0
    assert.throws(() => original.select({ runAttempt: 1 }, { runAttempt: 2 }, {}, original.TARGETS[0]),
      /current physical attempt/)
    assert.equal(effects, 0)
  })

  test('native originals prepare the host once and require a real current preparation execution', async t => {
    const f = fixture(t), preparing = f.prepareJob()
    const first = await original.plan(f.context, preparing.id)
    assert.deepEqual(first.targets, original.TARGETS.map(target => ({ target, action: 'build' })))
    assert.equal(f.hostBuilds, 1)
    f.identity.runAttempt = 2
    const second = await original.plan(f.context, f.prepareJob().id)
    assert.equal(f.hostBuilds, 1)
    assert.equal(second.runAttempt, 2)
    await assert.rejects(original.plan(f.context, preparing.id))
  })

  test('native originals preserve each successful sibling while a failed target has no build output', async t => {
    const f = fixture(t)
    for (const target of original.TARGETS.slice(0, 2)) f.add(target)
    const failed = job(f.identity, original.TARGETS[2], 800, 1, ['Setup Rust'])
    failed.conclusion = 'failure'; failed.steps[0].conclusion = 'failure'; f.rows.push(failed)
    const retained = await original.collect(f.context)
    assert.equal(retained.length, 2)
    for (const value of retained) assert.deepEqual(f.assets.get(value.archive.name).bytes,
      fs.readFileSync(f.artifacts.get(value.proof.artifact.id).file), 'Original Actions ZIP is copied without recompression')
    f.identity.runAttempt = 2
    const retry = await original.plan(f.context, f.prepareJob().id)
    assert.deepEqual(retry.targets.map(value => value.action), ['retain', 'retain', 'build'])
    assert.equal(f.hostBuilds, 1)
  })

  test('native originals stop when a previous success loses its archive or checkpoint identity', async t => {
    for (const corrupt of [f => f.assets.delete(original.archiveName(original.TARGETS[0])),
      f => { f.assets.get(original.archiveName(original.TARGETS[0])).metadata.id++ }]) {
      const f = fixture(t); f.add(original.TARGETS[0]); await original.collect(f.context)
      corrupt(f); f.identity.runAttempt = 2; const writes = f.uploads.length
      await assert.rejects(original.plan(f.context, f.prepareJob().id))
      assert.equal(f.uploads.length, writes, 'No rebuild, replacement or host repackaging after original loss')
    }
  })

  test('native originals survive Actions expiry only after durable originals were authenticated', async t => {
    const f = fixture(t), built = f.add(original.TARGETS[0]); await original.collect(f.context)
    f.artifacts.get(built.artifactId).metadata.expired = true; f.identity.runAttempt = 2
    const retry = await original.plan(f.context, f.prepareJob().id)
    assert.equal(retry.targets[0].action, 'retain')
    const lost = fixture(t), other = lost.add(original.TARGETS[0])
    lost.artifacts.get(other.artifactId).metadata.expired = true
    await assert.rejects(original.collect(lost.context))
    assert.equal(lost.uploads.length, 0)
  })

  test('native originals identify only complete cross-attempt server projections and reject physical replacements', t => {
    const f = fixture(t), built = f.add(original.TARGETS[0]).physical
    f.identity.runAttempt = 2
    const projected = { ...copy(built), id: 700, run_attempt: 2 }
    assert.equal(original.history([built, projected], f.identity, original.TARGETS[0]).physical.id, built.id)
    for (const mutate of [value => { value.run_attempt = 1 }, value => { value.completed_at = timestamp(21) },
      value => { value.steps[1].started_at = timestamp(0) }, value => { value.steps[1].conclusion = 'failure' },
      value => { value.status = 'in_progress' }, value => { value.head_sha = '9'.repeat(40) }]) {
      const changed = copy(projected); mutate(changed)
      assert.throws(() => original.history([built, changed], f.identity, original.TARGETS[0]))
    }
  })

  test('native originals finalizer restores all three raw binaries and the once-prepared host without a packager', async t => {
    const f = fixture(t); const preparing = f.prepareJob(); await original.plan(f.context, preparing.id)
    for (const target of original.TARGETS) f.add(target)
    await original.collect(f.context)
    f.context.buildHost = () => { throw Error('finalizer tried to package') }
    const output = path.join(f.root, 'final')
    const manifest = await original.assemble(f.context, output)
    assert.deepEqual(manifest.artifacts.map(value => value.target), original.TARGETS)
    assert.equal(f.hostBuilds, 1)
    sidecar.validateManifestV2(manifest, f.identity, original.TARGETS.map(target =>
      JSON.parse(f.assets.get(original.checkpointName(target)).bytes).build))
    assert.equal(fs.readdirSync(output).length, 6)
    const assets = fs.readdirSync(output).map((name, index) => ({ id: 10000 + index, name, state: 'uploaded',
      size: fs.statSync(path.join(output, name)).size, digest: `sha256:${sidecar.fileIdentity(path.join(output, name)).sha256}` }))
    const evidence = original.completion(f.identity, { ...f.release, draft: false }, assets,
      fs.readFileSync(path.join(output, sidecar.MANIFEST)))
    assert.equal(evidence.sourceRevision, f.identity.sourceRevision)
    assert.equal(evidence.physicalAttempt, 1); assert.equal(evidence.assets.length, 6)
    assert.equal(Object.hasOwn(evidence, 'authentication'), false)
    assert.equal(JSON.stringify(evidence).includes(key), false, 'Public completion does not carry the HMAC key')
    const old = JSON.parse(fs.readFileSync(path.join(output, sidecar.PROVENANCE)))
    delete old.manifestBytesBase64
    const oldBytes = Buffer.from(JSON.stringify(core.signReceipt(old, key)))
    const recoveryOutput = path.join(f.root, 'unserialized'); fs.mkdirSync(recoveryOutput)
    await assert.rejects(sidecar.recoverPublishedRelease({ identity: f.identity, release: {
      assets: [{ name: sidecar.PROVENANCE }] }, outputDirectory: recoveryOutput, sourceRoot: f.root,
      frontendBytes: f.preparation.receipt.frontendBytes, signingKey: key, contractVersion: 2,
      originalsOnly: true, readAsset: async () => oldBytes }), /serialized original manifest/)
  })

  test('native originals refuse host repackaging after a prepared checkpoint is lost', async t => {
    const f = fixture(t); await original.plan(f.context, f.prepareJob().id)
    f.assets.delete(original.HOST_CHECKPOINT); f.identity.runAttempt = 2
    await assert.rejects(original.plan(f.context, f.prepareJob().id))
    assert.equal(f.hostBuilds, 1)
  })

  test('native originals refuse foreign controller runs before any preservation write', async t => {
    for (const mutate of [run => { run.path = '.github/workflows/foreign.yml' },
      run => { run.head_sha = '1'.repeat(40) }, run => { run.run_attempt++ }]) {
      const f = fixture(t); const read = f.context.readRun
      f.context.readRun = async () => { const run = await read(); mutate(run); return run }
      await assert.rejects(original.plan(f.context, f.prepareJob().id))
      assert.equal(f.uploads.length, 0)
    }
  })

  test('native originals refuse replacing a missing checkpoint while its original ZIP remains', async t => {
    const f = fixture(t); f.add(original.TARGETS[0]); await original.collect(f.context)
    f.assets.delete(original.checkpointName(original.TARGETS[0])); f.identity.runAttempt = 2
    const writes = f.uploads.length
    await assert.rejects(original.plan(f.context, f.prepareJob().id))
    assert.equal(f.uploads.length, writes)
  })

  test('native originals authenticate the current physical preparation log and reject a copied logical plan', async t => {
    const f = fixture(t), preparing = f.prepareJob(), value = await original.plan(f.context, preparing.id)
    preparing.status = 'completed'; preparing.conclusion = 'success'
    preparing.steps[0].status = 'completed'; preparing.steps[0].conclusion = 'success'
    original.emit(original.PLAN_PREFIX, value, line => f.logs.set(preparing.id, `${timestamp(19)} ${line}\n`))
    const current = job(f.identity, original.TARGETS[0], 777, 1, [original.SELECT])
    current.status = 'in_progress'; current.conclusion = null
    current.steps[0].status = 'in_progress'; current.steps[0].conclusion = null; f.rows.push(current)
    assert.equal((await original.selectCurrent(f.context, value, original.TARGETS[0], current)).action, 'build')
    const changed = copy(value); changed.preparationJobId++
    await assert.rejects(original.selectCurrent(f.context, changed, original.TARGETS[0], current))
    f.logs.set(preparing.id, `${timestamp(19)} ${original.PLAN_PREFIX}${Buffer.from(JSON.stringify(changed)).toString('base64')}\n`)
    await assert.rejects(original.selectCurrent(f.context, value, original.TARGETS[0], current))
  })

  test('native originals plan reader accepts a large whole-job prefix while bounding the single plan frame', async t => {
    const f = fixture(t), preparing = f.prepareJob(), value = await original.plan(f.context, preparing.id)
    preparing.status = 'completed'; preparing.conclusion = 'success'
    preparing.steps[0].status = 'completed'; preparing.steps[0].conclusion = 'success'
    const current = job(f.identity, original.TARGETS[0], 777, 1, [original.SELECT])
    current.status = 'in_progress'; current.conclusion = null
    current.steps[0].status = 'in_progress'; current.steps[0].conclusion = null; f.rows.push(current)
    let frame; original.emit(original.PLAN_PREFIX, value, line => { frame = `${timestamp(19)} ${line}\n` })
    const log = `${timestamp(10)} ${'legitimate package verification output '.repeat(18000)}\n${frame}`
    assert.ok(Buffer.byteLength(log) > 512 * 1024)
    await t.test('whole-job parser', () => assert.deepEqual(original.logRecord(log, original.PLAN_PREFIX), value))
    await t.test('read-only worker loader', async () => {
      const execute = sidecar.execute
      try {
        sidecar.execute = (program, args, options = {}) => {
          assert.equal(program, 'gh'); const route = args[1]
          if (route.includes('/attempts/1/jobs?')) return JSON.stringify({ total_count: f.rows.length, jobs: f.rows })
          if (route.endsWith(`/jobs/${current.id}`)) return JSON.stringify(current)
          if (route.endsWith(`/jobs/${preparing.id}`)) return JSON.stringify(preparing)
          assert.ok(route.endsWith(`/jobs/${preparing.id}/logs`))
          assert.ok(options.maxBuffer >= Buffer.byteLength(log), 'Whole-job stdout exceeded the read-only loader buffer')
          assert.ok(options.maxBuffer <= 64 * 1024 * 1024, 'Whole-job reads remain bounded')
          return log
        }
        assert.equal((await original.workerSelection(f.identity, f.preparation, value, original.TARGETS[0], true)).action, 'build')
      } finally { sidecar.execute = execute }
    })
    await t.test('single frame remains capped at 128 KiB', () => {
      const oversized = Buffer.from(JSON.stringify({ padding: 'x'.repeat(100000) })).toString('base64')
      assert.ok(oversized.length > 128 * 1024)
      assert.throws(() => original.logRecord(`${timestamp(19)} ${original.PLAN_PREFIX}${oversized}\n`, original.PLAN_PREFIX))
    })
  })

  test('native originals revalidate original bytes after a pure selector failure but finalization still requires latest success', async t => {
    const f = fixture(t); f.add(original.TARGETS[0]); await original.collect(f.context)
    f.identity.runAttempt = 3
    const failed = job(f.identity, original.TARGETS[0], 700, 2, [original.SELECT])
    failed.conclusion = 'failure'; failed.steps[0].conclusion = 'failure'; f.rows.push(failed)
    const value = await original.plan(f.context, f.prepareJob().id)
    assert.equal(value.targets[0].action, 'retain')
    assert.throws(() => original.history(f.rows, f.identity, original.TARGETS[0], { final: true }), /Latest logical/)
    const realFailure = copy(failed); realFailure.steps[0].name = original.BUILD
    assert.throws(() => original.history([f.rows[0], realFailure], f.identity, original.TARGETS[0]), /real failed/)
  })

  test('native originals retain valid siblings before reporting another expired original', async t => {
    const f = fixture(t); const expired = f.add(original.TARGETS[0])
    f.artifacts.get(expired.artifactId).metadata.expired = true
    f.add(original.TARGETS[1]); f.add(original.TARGETS[2])
    await assert.rejects(original.collect(f.context))
    assert.ok(!f.assets.has(original.checkpointName(original.TARGETS[0])))
    for (const target of original.TARGETS.slice(1)) assert.ok(f.assets.has(original.checkpointName(target)))
  })

  test('native originals reject unsafe archive members even when artifact and transport digests match', async t => {
    for (const kind of ['extra', 'duplicate', 'symlink', 'traversal', 'comment']) {
      const f = fixture(t)
      f.add(original.TARGETS[0], { zipMutate: zip => {
        const script = String.raw`import sys,zipfile,stat
p,kind=sys.argv[1:]; source=zipfile.ZipFile(p); members=[(i.filename,source.read(i)) for i in source.infolist()]; source.close()
with zipfile.ZipFile(p,'w',compression=zipfile.ZIP_DEFLATED) as z:
 for name,data in members:
  i=zipfile.ZipInfo('../escape' if kind=='traversal' and name=='build-receipt.json' else name)
  i.external_attr=(stat.S_IFLNK|0o777)<<16 if kind=='symlink' and name=='build-receipt.json' else (stat.S_IFREG|0o644)<<16
  z.writestr(i,data)
 if kind=='extra': z.writestr('extra','bad')
 if kind=='duplicate': z.writestr(members[0][0],members[0][1])
 if kind=='comment': z.comment=b'unexpected'
`
        const child = spawnSync(process.env.PYTHON || 'python3', ['-c', script, zip, kind],
          { env: { PATH: process.env.PATH }, encoding: 'utf8' })
        assert.equal(child.status, 0, child.stderr)
      } })
      await assert.rejects(original.collect(f.context))
      assert.equal(f.uploads.length, 0)
    }
  })

  test('native originals read back an uncertain upload once without replacing or recompressing bytes', async t => {
    const f = fixture(t); const built = f.add(original.TARGETS[0]); const upload = f.context.upload
    f.logs.set(built.physical.id, `${timestamp(10)} ${'verbose Cargo diagnostic '.repeat(25000)}\n${f.logs.get(built.physical.id)}`)
    let uncertain = true
    f.context.upload = async (...args) => { await upload(...args); if (uncertain) { uncertain = false; throw Error('unknown transport outcome') } }
    await original.collect(f.context)
    assert.equal(f.uploads.length, 2)
    assert.deepEqual(f.assets.get(original.archiveName(original.TARGETS[0])).bytes, fs.readFileSync(built.zip))
  })

  test('native originals reject changed source version tag reservation or freshness before writes', async t => {
    for (const mutate of [f => { f.context.guard = () => { throw Error('source changed') } },
      f => { f.release.target_commitish = '9'.repeat(40) }, f => { f.release.tag_name = 'v2026.10.999' },
      f => { f.context.readTag = async () => ({ object: { type: 'commit', sha: '9'.repeat(40) } }) },
      f => { const p = core.readBodyReceipt(f.release); p.frontendBytes.archiveSha256 = '9'.repeat(64); f.release.body = core.receiptBody(core.signReceipt(p, key)) },
      f => { const p = core.readBodyReceipt(f.release); delete p.authentication; f.release.body = core.receiptBody(p) }]) {
      const f = fixture(t); const preparing = f.prepareJob(); mutate(f)
      await assert.rejects(original.plan(f.context, preparing.id)); assert.equal(f.uploads.length, 0)
    }
  })

  test('native originals actual cache-policy entry stops copied preparation before cache output or Cargo', t => {
    const f = fixture(t), repo = path.join(f.root, 'source'), hooks = path.join(f.root, 'empty-hooks')
    fs.mkdirSync(repo); fs.mkdirSync(hooks); fs.mkdirSync(path.join(repo, 'scripts'))
    fs.writeFileSync(path.join(repo, 'Cargo.toml'), '[package]\nname="fixture"\nversion="0.0.0"\n')
    fs.writeFileSync(path.join(repo, 'scripts/sidecar-release.cjs'), '// BAMBOO_SHARED_EXECUTABLES_V2\n')
    const env = { PATH: process.env.PATH, HOME: f.root, GIT_CONFIG_NOSYSTEM: '1' }
    const git = (...args) => {
      const result = spawnSync('git', ['-c', `core.hooksPath=${hooks}`, '-c', 'commit.gpgsign=false',
        '-c', 'user.name=Fixture', '-c', 'user.email=fixture@invalid', ...args], { cwd: repo, env, encoding: 'utf8' })
      assert.equal(result.status, 0, result.stderr); return result.stdout.trim()
    }
    git('init', '-q'); git('add', '.'); git('commit', '-qm', 'fixture')
    const source = git('rev-parse', 'HEAD'), tree = git('rev-parse', 'HEAD^{tree}')
    const identity = { ...f.identity, sourceRevision: source, sourceTree: tree }
    const receipt = copy(f.preparation.receipt); receipt.identity.sourceRevision = source
    const preparation = core.createPreparation({ release: { ...f.release, target_commitish: source },
      receipt: core.signReceipt(receipt, key) }, identity, 2)
    const event = path.join(f.root, 'event.json'), output = path.join(f.root, 'output')
    fs.writeFileSync(event, JSON.stringify({ workflow_run: { name: 'CI', event: 'push', conclusion: 'success',
      head_branch: 'main', head_sha: source, repository: { full_name: identity.repository },
      head_repository: { full_name: identity.repository }, html_url: identity.ciRun } }))
    const workerEnv = { ...env, RUNNER_TEMP: f.root, GITHUB_REPOSITORY: identity.repository,
        GITHUB_EVENT_NAME: 'workflow_run', GITHUB_EVENT_PATH: event, GITHUB_OUTPUT: output,
        GITHUB_RUN_ID: identity.runId, GITHUB_RUN_ATTEMPT: '2', GITHUB_WORKFLOW_SHA: identity.workflowRevision, GITHUB_SHA: identity.workflowRevision,
        SOURCE_REVISION: source, TARGET_VERSION: identity.version, BAMBOO_EXECUTABLE_CONTRACT_VERSION: '2',
        BAMBOO_RELEASE_TARGET: require('./sidecar-build.cjs').nativeTarget(),
        PREPARED_RELEASE: JSON.stringify(preparation), NATIVE_ORIGINAL_PLAN: JSON.stringify({ runAttempt: 1 }) }
    for (const [changed, expected] of [[{}, /current physical attempt/], [{ GITHUB_SHA: source }, /must not be relabeled/]]) {
      const result = spawnSync(process.execPath, [path.join(__dirname, 'sidecar-build.cjs'), 'cache-policy'], {
        cwd: repo, encoding: 'utf8', env: { ...workerEnv, ...changed } })
      assert.notEqual(result.status, 0); assert.match(result.stderr, expected)
    }
    assert.equal(fs.existsSync(output), false, 'Cache-policy did not write its workspace/key output')
    assert.equal(fs.existsSync(path.join(f.root, 'bamboo-executable-target')), false)
  })
}

module.exports = { register }
if (require.main === module) register()
