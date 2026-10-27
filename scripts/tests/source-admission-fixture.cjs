"use strict";
// Inert source API fixture shared by controller and read-only promotion tests.
// It models transport only; it is never a complete accepted producer registry.
const assert = require("node:assert/strict");
const { createHash } = require("node:crypto");
const frozenSource = require("../frozen-source-admission.cjs");
const frozenFixture = require("./frozen-source-admission-fixtures.json");
const sourceHash = bytes => createHash("sha256").update(bytes).digest("hex");
function frozenSourceFixture(identities = frozenFixture.identities) {
  const { B, F, M, S, C, anchor, devAdvance } = identities
  const repository = { id: 1164779771, full_name: "bigduu/Bamboo-agent" }, times = frozenFixture.times
  const model = {
    input: { sourceRevision: S, releaseRef: `release/${S}`, featurePr: 2333, ciRun: 123 },
    pr: { number: 2333, state: "closed", merged: true, draft: false, merged_at: times.merged, merge_commit_sha: S,
      base: { ref: "dev", sha: devAdvance, repo: repository }, head: { ref: "feature/inert-fixture", sha: F, repo: repository } },
    run: { id: 123, run_attempt: 1, workflow_id: 456, check_suite_id: 789, check_suite_node_id: "CS_fixture",
      repository, head_repository: repository, event: "pull_request", head_sha: F, head_branch: "feature/inert-fixture",
      name: "CI", path: frozenSource.WORKFLOW, status: "completed", conclusion: "success", pull_requests: [],
      created_at: times.created, updated_at: times.completed },
    main: { name: "main", protected: true, commit: { sha: C } },
    dev: { name: "dev", protected: true, commit: { sha: S }, protection: { required_status_checks: {
      enforcement_level: "everyone", contexts: ["Test", "E2E Tests"], checks: [
        { context: "Test", app_id: 15368 }, { context: "E2E Tests", app_id: 15368 }] } } },
    release: { name: `release/${S}`, protected: false, commit: { sha: S } },
    accepted: { sha: S, parents: [{ sha: B }, { sha: F }], tree: { sha: "e".repeat(40) } },
    snapshot: { sha: M, parents: [{ sha: B }, { sha: F }], tree: { sha: "e".repeat(40) } },
    workflow: { id: 456, path: frozenSource.WORKFLOW, state: "active" }, calls: [], files: {},
    sourceLog: `${times.started} Event source: ${M}; full validation: false\n`,
  }
  model.suite = { databaseId: 789, repository: { databaseId: repository.id, nameWithOwner: repository.full_name },
    app: { databaseId: 15368, slug: "github-actions" }, commit: { oid: F }, workflowRun: {
      databaseId: 123, event: "pull_request", runAttempt: 1, workflow: { databaseId: 456, name: "CI" },
      file: { path: frozenSource.WORKFLOW, repositoryName: repository.full_name,
        repositoryFileUrl: `https://github.com/${repository.full_name}/blob/${F}/${frozenSource.WORKFLOW}` } } }
  model.physical = frozenFixture.jobs.map((name, index) => ({ id: 100 + index, run_id: 123, run_attempt: 1,
    name, head_sha: F, head_branch: model.pr.head.ref, status: "completed", conclusion: "success",
    started_at: times.started, completed_at: times.completed, check_run_url: `https://api.github.com/repos/${repository.full_name}/check-runs/${100 + index}`,
    steps: [{ name: index ? "Execute required acceptance" : "Bind full validation to the actual event snapshot", number: 1,
      status: "completed", conclusion: "success", started_at: times.started, completed_at: times.completed }] }))
  model.latest = structuredClone(model.physical); model.attempts = { 1: structuredClone(model.physical) }
  const zip = "inert ZIP bytes: never downloaded as executable code"
  const lock = { schemaVersion: 1, packageName: "@bigduu/lotus-next", packageVersion: "2026.10.27",
    sourceRevision: "a".repeat(40), sourceDirty: false, entrypoint: "index.html", resourcesSha256: "8".repeat(64), manifestSha256: "9".repeat(64) }
  const content = { [frozenSource.WORKFLOW]: frozenFixture.workflowText, "Cargo.lock": "# inert locked fixture\n",
    "scripts/frontend-package-lock.json": JSON.stringify(lock),
    [frozenSource.INPUT_PATHS.frontendManifest]: JSON.stringify({ schema_version: 1, frontend_name: "lotus-next", frontend_version: lock.packageVersion, entry: "index.html", bundle_hash: `sha256:${'7'.repeat(64)}` }),
    [frozenSource.INPUT_PATHS.frontendZip]: zip, [frozenSource.INPUT_PATHS.nativeContract]: "// BAMBOO_SHARED_EXECUTABLES_V2\n" }
  const helpers = Object.fromEntries(frozenSource.REQUIRED_HELPERS.map(path => [path, sourceHash(frozenFixture.helperText)]))
  for (const path of frozenSource.REQUIRED_HELPERS) content[path] = frozenFixture.helperText
  for (const revision of [F, M, S]) model.files[revision] = structuredClone(content)
  const contract = { complete: true, workflowId: 456, coverage: [...frozenSource.COVERAGE], helpers,
    requiredJobs: [...frozenFixture.jobs], requiredSteps: Object.fromEntries(model.physical.map(job => [job.name, job.steps.map(step => step.name)])) }
  model.contract = contract
  model.policy = { mainHistoryAnchor: anchor, producers: { [sourceHash(frozenFixture.workflowText)]: contract }, environment: {
    GITHUB_WORKFLOW_SHA: C, GITHUB_SHA: C, GITHUB_EVENT_NAME: "workflow_dispatch", GITHUB_REPOSITORY: repository.full_name,
    GITHUB_REF: "refs/heads/main", GITHUB_REF_PROTECTED: "true", GITHUB_WORKFLOW_REF: `${repository.full_name}/${frozenSource.CONTROLLER}@refs/heads/main` } }
  const clone = value => structuredClone(value)
  const gitBlob = (path, ref) => {
    const bytes = Buffer.from(model.files[ref][path]), sha = createHash("sha1").update(`blob ${bytes.length}\0`).update(bytes).digest("hex")
    return { path, sha, size: bytes.length, encoding: "base64", content: bytes.toString("base64") }
  }
  model.reads = {
    json(route) {
      model.calls.push(route); const nth = model.calls.filter(item => item === route).length
      let result
      if (route === "branches/main") result = model.main
      else if (route === "branches/dev") result = model.advanceDev && nth > 1 ? { ...model.dev, commit: { sha: devAdvance } } : model.dev
      else if (route === `branches/${encodeURIComponent(model.input.releaseRef)}`) result = model.release
      else if (route === `pulls/${model.input.featurePr}`) result = model.pr
      else if (route === `actions/runs/${model.run.id}`) result = model.run
      else if (route === `git/commits/${S}`) result = model.accepted
      else if (route === `git/commits/${M}`) result = model.snapshot
      else if (route.startsWith("compare/")) {
        const [base, head] = route.slice(8).split("...")
        result = { status: base === head ? "identical" : "ahead", base_commit: { sha: base }, merge_base_commit: { sha: base } }
        if ((model.rewriteDev && base === S && head === devAdvance) || (model.missingMainHistory && base === anchor)) result.status = "diverged"
      } else if (route.startsWith("contents/")) {
        const [path, ref] = route.slice(9).split("?ref="); result = gitBlob(path, ref)
        if (model.largeBlob && path === frozenSource.INPUT_PATHS.frontendZip) result = { ...result, content: "", encoding: "none" }
      } else if (route.startsWith("git/blobs/")) {
        result = Object.keys(content).map(path => gitBlob(path, S)).find(value => value.sha === route.slice(10)); assert.ok(result)
      } else if (route === "actions/workflows/456") result = model.workflow
      else if (route.startsWith("actions/workflows/456/runs?")) result = { workflow_runs: model.newerRun || (model.raceNewRun && nth > 1) ? [model.run, { ...model.run, id: 124, created_at: times.started, conclusion: "failure" }] : [model.run] }
      else if (route.startsWith("actions/runs/123/jobs?")) result = { jobs: route.includes("filter=all") ? Object.values(model.attempts).flat() : model.latest }
      else if (route.startsWith("actions/runs/123/attempts/")) result = { jobs: model.attempts[route.match(/attempts\/(\d+)/)[1]] ?? [] }
      else if (route.startsWith("actions/jobs/")) result = model.physical.find(job => job.id === Number(route.slice(13)))
      else assert.fail(`Unexpected source resolver read: ${route}`)
      if (model.apiFailure && route === "actions/jobs/100") throw Error("Original API evidence unavailable")
      result = clone(result)
      for (const key of ["jobs", "workflow_runs"]) if (result[key]) {
        const all = result[key], page = Number(route.match(/&page=(\d+)$/)[1])
        result.total_count = all.length; result[key] = all.slice((page - 1) * 100, page * 100)
        if (model.missingTotal) delete result.total_count
        if ((model.truncatedRuns && key === "workflow_runs") || (model.truncatedJobs && key === "jobs")) result.total_count++
        if (model.changedPageTotal && page > 1) result.total_count++
      }
      if (nth > 1) {
        if (model.raceRun && route === "actions/runs/123") result.run_attempt++
        if (model.raceRef && route.startsWith("branches/release")) result.commit.sha = "b".repeat(40)
        if (model.raceJobs && route.startsWith("actions/runs/123/jobs?")) result.jobs[0].conclusion = "failure"
        if (model.racePr && route.startsWith("pulls/")) result.merged = false
        if (model.raceMergedAt && route.startsWith("pulls/")) result.merged_at = times.created
      }
      return result
    },
    checkSuite(id) {
      assert.equal(id, "CS_fixture"); model.suiteReads = (model.suiteReads ?? 0) + 1
      const result = clone(model.suite); if (model.raceSuite && model.suiteReads > 1) result.commit.oid = "a".repeat(40)
      return result
    },
    jobLog(id) { assert.equal(id, 100); return model.sourceLog },
  }
  model.resolve = () => frozenSource.resolveFrozenSource(model.input, model.reads, model.policy)
  return model
}
module.exports = { frozenSourceFixture };
