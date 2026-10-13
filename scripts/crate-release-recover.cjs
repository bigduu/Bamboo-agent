#!/usr/bin/env node
// A bounded replay of an already-authorized main CI publication, never a new version.
const assert = require('node:assert/strict')
const fs = require('node:fs')
const core = require('./crate-release.cjs')

const TARGET = Object.freeze({ repository: 'bigduu/Bamboo-agent',
  source: '430bf3b5a1e10305fd4226b81b13f6a2c28afa8e', version: '2026.10.11',
  releaseId: 411697635, ciRunId: 38396650596,
  ciUrl: 'https://github.com/bigduu/Bamboo-agent/actions/runs/38396650596' })
const SHA = /^[0-9a-f]{40}$/
const RELEASES = Symbol('original recovery release reader')

function api(route) {
  return JSON.parse(core.command(['gh', 'api', `repos/${TARGET.repository}/${route}`]))
}

function validateRequest(env, source) {
  assert.equal(env.GITHUB_REPOSITORY, TARGET.repository)
  assert.equal(env.GITHUB_EVENT_NAME, 'workflow_dispatch')
  assert.ok(['refs/heads/dev', 'refs/heads/main'].includes(env.GITHUB_REF))
  assert.equal(env.GITHUB_REF_TYPE, 'branch')
  assert.equal(env.GITHUB_REF_PROTECTED, 'true')
  assert.match(env.EXPECTED_CONTROLLER_SHA, SHA)
  assert.equal(env.GITHUB_SHA, env.EXPECTED_CONTROLLER_SHA)
  assert.equal(env.GITHUB_WORKFLOW_SHA, env.EXPECTED_CONTROLLER_SHA)
  assert.equal(env.GITHUB_WORKFLOW_REF,
    `${TARGET.repository}/.github/workflows/recover-crate-release.yml@${env.GITHUB_REF}`)
  assert.equal(source, TARGET.source, 'Recovery must package only the original main CI source')
}

function authorize(env = process.env, read = api, source = core.command(['git', 'rev-parse', 'HEAD']).trim()) {
  validateRequest(env, source)
  const run = read(`actions/runs/${TARGET.ciRunId}`)
  assert.equal(run.id, TARGET.ciRunId)
  assert.equal(run.name, 'CI')
  assert.equal(run.path, '.github/workflows/ci.yml')
  assert.equal(run.status, 'completed')
  assert.equal(run.conclusion, 'success')
  assert.equal(run.event, 'push')
  assert.equal(run.head_branch, 'main')
  assert.equal(run.head_sha, TARGET.source)
  assert.equal(run.repository.full_name, TARGET.repository)
  assert.equal(run.head_repository.full_name, TARGET.repository)
  assert.equal(run.html_url, TARGET.ciUrl)
  const tips = ['dev', 'main'].map(name => {
    const branch = read(`branches/${name}`)
    assert.equal(branch.name, name)
    assert.equal(branch.protected, true)
    assert.match(branch.commit.sha, SHA)
    return branch.commit.sha
  })
  for (const revision of [source, env.EXPECTED_CONTROLLER_SHA]) {
    assert.ok(tips.some(tip => {
      const comparison = read(`compare/${revision}...${tip}`)
      return ['ahead', 'identical'].includes(comparison.status) && comparison.merge_base_commit.sha === revision
    }), 'Source and controller must belong to protected history')
  }
  return run
}

async function requireReservation(context, release) {
  assert.equal(release.id, TARGET.releaseId, 'Unexpected reservation ID')
  assert.equal(release.tag_name, `v${TARGET.version}`)
  assert.equal(release.target_commitish, TARGET.source)
  assert.equal(release.prerelease, false)
  const receipt = await context.readReceipt(release, {required: true})
  assert.ok(receipt, 'Existing authenticated reservation is required')
  context.verifyReceipt(receipt)
  core.validateReceipt(receipt, context.identity, context.crates, TARGET.version)
  assert.equal(receipt.automatic, true)
  assert.equal(receipt.ciRun, TARGET.ciUrl)
  assert.ok(release.draft || receipt.completed)
  assert.equal(context.identity.sourceRevision, TARGET.source)
  return receipt
}

function constrain(context, read = api) {
  context.reserve = async () => { throw new Error('Recovery cannot create a reservation or allocate a version') }
  if (!context[RELEASES]) context[RELEASES] = context.releases.bind(context)
  context.releases = async () => {
    const entries = await context[RELEASES]()
    const candidates = entries.filter(entry => entry.tag_name === `v${TARGET.version}`)
    assert.equal(candidates.length, 1, 'Existing reservation must appear exactly once')
    await requireReservation(context, candidates[0])
    return entries
  }
  context.ensureTag = async () => {
    const tag = read(`git/ref/tags/v${TARGET.version}`)
    assert.equal(tag.object.type, 'commit')
    assert.equal(tag.object.sha, TARGET.source, 'Existing immutable tag must match source430')
  }
  return context
}

async function recoveryPlan(context, read = api) {
  constrain(context, read)
  const existing = read(`releases/${TARGET.releaseId}`)
  await requireReservation(context, existing)
  await context.ensureTag()
  const result = await core.plan(context)
  assert.equal(result.release.id, TARGET.releaseId)
  assert.equal(result.receipt.version, TARGET.version)
  await requireReservation(context, result.release)
  return result
}

async function recoveryPublish(context, receipt, read = api) {
  constrain(context, read)
  const release = read(`releases/${TARGET.releaseId}`)
  const authenticated = await requireReservation(context, release)
  assert.deepEqual(receipt, authenticated, 'The planned receipt changed before upload')
  await context.ensureTag()
  await core.publish(context, release, receipt)
}


async function main() {
  const operation = process.argv[2]
  if (operation === 'authorize') {
    authorize()
    assert.equal(core.command(['git', 'status', '--porcelain', '--untracked-files=all']), '')
    console.log(`Authorized controller ${process.env.EXPECTED_CONTROLLER_SHA}; product ${TARGET.source}; CI ${TARGET.ciRunId}`)
    return
  }
  const context = core.makeRecoveryContext()
  if (operation === 'plan') {
    const result = await recoveryPlan(context)
    fs.writeFileSync(context.receiptFile, `${JSON.stringify(result.receipt, null, 2)}\n`)
    fs.appendFileSync(process.env.GITHUB_OUTPUT, `version=${TARGET.version}\nreceipt_file=${context.receiptFile}\n`)
    console.log(`Recovery uses existing draft ${TARGET.releaseId} version ${TARGET.version}; no allocation`)
  } else {
    assert.equal(operation, 'publish')
    const receipt = JSON.parse(fs.readFileSync(context.receiptFile, 'utf8'))
    await recoveryPublish(context, receipt)
  }
}

module.exports = {TARGET, validateRequest, authorize, requireReservation, constrain, recoveryPlan, recoveryPublish}
if (require.main === module) main().catch(error => { console.error(error); process.exitCode = 1 })
