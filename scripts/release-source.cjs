const assert = require('node:assert/strict')

// The owner-requested dispatch publishes the original completed CI tree.
// Updated workflow controllers execute outside that immutable product tree.
const REQUIRED = Object.freeze(['Validation Source', 'Promotion Source', 'Test', 'E2E Tests', 'Lint',
  'Build (ubuntu-latest)', 'Build (macos-latest)', 'Build (windows-latest)'])
const SHA = /^[0-9a-f]{40}$/

function validateDispatch(event, validation, sourceRevision, sourceTree) {
  assert.ok(event.inputs?.complete_release === 'true' || event.inputs?.complete_release === true)
  assert.equal(event.inputs.expected_source_sha, sourceRevision)
  assert.match(sourceRevision, SHA)
  assert.match(sourceTree, SHA)
  assert.equal(validation?.kind, 'bamboo-release-ci-reuse/v1')
  assert.equal(validation.sourceRevision, sourceRevision)
  assert.equal(validation.sourceTree, sourceTree)
  const run = validation.ciRun
  assert.equal(String(run?.id), event.inputs.accepted_ci_run)
  assert.equal(run.name, 'CI')
  assert.equal(run.path, '.github/workflows/ci.yml')
  assert.equal(run.status, 'completed')
  assert.equal(run.conclusion, 'success')
  assert.equal(run.event, 'pull_request')
  assert.equal(run.repository?.full_name, 'bigduu/Bamboo-agent')
  assert.equal(run.head_repository?.full_name, 'bigduu/Bamboo-agent')
  assert.match(run.head_branch, /^release\/[0-9a-f]{40}$/)
  assert.match(run.head_sha, SHA)
  assert.equal(sourceRevision, run.head_sha, 'Publish the original validated source')
  assert.equal(run.html_url, `https://github.com/bigduu/Bamboo-agent/actions/runs/${run.id}`)
  assert.match(validation.validatedRevision, SHA)
  assert.match(validation.validatedTree, SHA)
  assert.deepEqual(validation.requiredChecks, REQUIRED)
  assert.ok(Array.isArray(validation.policyOnlyPaths))
  assert.deepEqual(validation.policyOnlyPaths, [], 'No changed product source may reuse earlier CI')
  assert.equal(sourceTree, validation.validatedTree, 'Require the exact accepted CI tree')
  const promotion = validation.mainPromotion
  assert.equal(promotion?.mainProtected, true, 'Require protected main promotion before publication')
  assert.equal(promotion.sourceInMain, true)
  assert.equal(promotion.reviewedBeforeMerge, true, 'Require review completion before normal main promotion')
  assert.match(promotion.mergeRevision, SHA)
  assert.equal(promotion.mergeTree, sourceTree)
  assert.equal(promotion.mergeParents?.length, 2)
  for (const parent of promotion.mergeParents) assert.match(parent, SHA)
  assert.ok(promotion.mergeParents.includes(sourceRevision))
  const pr = promotion.pullRequest
  assert.ok(Number.isSafeInteger(pr?.number) && pr.number > 0)
  assert.equal(pr.html_url, `https://github.com/bigduu/Bamboo-agent/pull/${pr.number}`)
  assert.equal(pr.state, 'closed')
  assert.equal(pr.merged, true)
  assert.equal(pr.draft, false)
  assert.equal(pr.baseRef, 'main')
  assert.equal(pr.headRef, run.head_branch)
  assert.equal(pr.headRevision, sourceRevision)
  assert.ok(typeof pr.mergedAt === 'string' && Number.isFinite(Date.parse(pr.mergedAt)))
  const summary = promotion.reviewSummary
  // The preflight logs the actual comment URL; stable recovery binds its verified result.
  assert.equal(summary?.actor?.type, 'Bot')
  assert.ok(['chatgpt-codex-connector[bot]', 'chatgpt-codex-connector'].includes(summary.actor.login))
  for (const kind of ['code_review', 'security_review']) {
    const review = summary.state[kind]
    assert.equal(review?.commit_sha, sourceRevision)
    assert.equal(review.status, 'completed')
    assert.equal(review.has_any_findings, false)
    assert.equal(review.has_unresolved_findings, false)
  }
  // Distinct HEAD-derived metadata is recorded, never claimed byte-identical.
  assert.equal(validation.productTreeVerified, true)
  return run
}

module.exports = { REQUIRED, validateDispatch }
