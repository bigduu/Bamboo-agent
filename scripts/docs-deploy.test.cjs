"use strict";
const assert = require("node:assert/strict");
const { test } = require("node:test");
const admission = require("./main-admission.cjs");
const fs = require("node:fs"), os = require("node:os"), path = require("node:path");
const { spawnSync } = require("node:child_process"), { runInThisContext } = require("node:vm");
const core = require("./crate-release.cjs"), source = require("./frozen-source-admission.cjs"), proof = require("./published-source-admission.cjs");
const implementation = require("./docs-deploy.cjs");

// Transport contracts only. Every subprocess/network port is inert; these
// fixtures never provide Source, publication or Pages authorization.
const environment = { PATH: "/inert/bin", HOME: "/inert/home", TMPDIR: "/inert/tmp", GH_TOKEN: "inert-read-token",
  BAMBOO_RELEASE_TOKEN: "unused", BAMBOO_RELEASE_SIGNING_KEY: "unused", CARGO_REGISTRY_TOKEN: "unused",
  ACTIONS_ID_TOKEN_REQUEST_TOKEN: "unused", NODE_OPTIONS: "unused" };
const noNetwork = () => assert.fail("No network request is permitted in this transport contract");

function register() {
  test("Docs2355 artifact transport preserves binary bytes on one fixed read-only repository route", async () => {
    const bytes = Buffer.from([80, 75, 3, 4, 0, 255, 128, 10]), calls = [];
    const reads = admission.createReads(environment, (command, args, options) => {
      calls.push({ command, args, options }); return { status: 0, stdout: Buffer.from(bytes) };
    }, noNetwork);
    assert.deepEqual(await reads.artifactBytes(71, bytes.length), bytes);
    assert.equal(calls.length, 1); const call = calls[0];
    assert.equal(call.command, "gh");
    assert.deepEqual(call.args, ["api", "--hostname", "github.com", "--method", "GET",
      "repos/bigduu/Bamboo-agent/actions/artifacts/71/zip"]);
    assert.equal(call.options.encoding, null); assert.equal(call.options.maxBuffer, bytes.length);
    assert.equal(call.options.timeout, 30000);
    assert.deepEqual(call.options.env, { PATH: environment.PATH, HOME: environment.HOME, TMPDIR: environment.TMPDIR,
      GH_TOKEN: environment.GH_TOKEN, GH_PROMPT_DISABLED: "1" });
  });

  test("Docs2355 artifact transport rejects invalid locators, excessive bounds and unknown read outcomes", async () => {
    let calls = 0;
    const reads = admission.createReads(environment, () => { calls++; return { status: 0, stdout: Buffer.alloc(8) }; }, noNetwork);
    for (const id of [0, -1, "1e2", "../71", "https://example.invalid", NaN, Number.MAX_SAFE_INTEGER + 1]) {
      await assert.rejects(reads.artifactBytes(id, 8));
    }
    for (const maximum of [undefined, 0, -1, "8", Infinity, 1024 ** 3 + 1]) {
      await assert.rejects(reads.artifactBytes(71, maximum));
    }
    assert.equal(calls, 0, "Invalid inputs must stop before any command");
    for (const outcome of [{ status: 1, stdout: Buffer.alloc(0) }, { error: Error("inert transport error"), status: null },
      { status: 0, stdout: Buffer.alloc(9) }]) {
      const failed = admission.createReads(environment, () => outcome, noNetwork);
      await assert.rejects(failed.artifactBytes(71, 8), /Read-only GitHub request failed|Oversized GitHub response/);
    }
  });

  test("Docs2355 public read factory adds only artifact bytes and accepts no policy or credentials", () => {
    const reads = admission.createPublicReads();
    assert.deepEqual(Object.keys(reads).sort(), ["artifactBytes", "assetBytes", "checkSuite", "jobLog", "json", "registryChecksum"]);
    for (const value of Object.values(reads)) assert.equal(typeof value, "function");
    for (const override of [null, environment, { policy: {} }, { authority: true }]) {
      assert.throws(() => admission.createPublicReads(override), /no policy, authority or credential override/);
    }
  });
  registerConsumer();
}

const time = second => `2026-10-27T00:00:${String(second).padStart(2, "0")}Z`;
const copy = value => JSON.parse(JSON.stringify(value));
function fixture(t, change = "") {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "bamboo-docs-consumer-")));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const sourceFiles = {}, read = name => { const bytes = fs.readFileSync(name); sourceFiles[name] = { sha256: core.sha256(bytes), text: bytes.toString("utf8") }; };
  read("Cargo.toml"); read("Cargo.lock"); read(".cargo/config.toml"); read(".github/workflows/ci.yml");
  for (const match of sourceFiles["Cargo.toml"].text.match(/default-members = \[([\s\S]*?)\]/)[1].matchAll(/^\s*"([^"]+)",/gm)) {
    read(match[1] === "." ? "Cargo.toml" : match[1] + "/Cargo.toml");
  }
  const expected = { snapshot: "2".repeat(40), tree: "3".repeat(40), workflowDefinition: "4".repeat(40),
    pr: 99, run: 100, attempt: 1, producerDigest: sourceFiles[".github/workflows/ci.yml"].sha256, sourceFiles };
  // Only this tiny fixture creates archives. Consumer production has no archive
  // writer; all real Python execution below is fixed C data verification.
  const build = String.raw`
import hashlib,io,json,pathlib,stat,sys,tarfile,tomllib,zipfile
root=pathlib.Path(sys.argv[1]); change=sys.argv[2]; e=json.load(sys.stdin); files=e['sourceFiles']
digest=lambda b:hashlib.sha256(b).hexdigest()
members=[]
for member in tomllib.loads(files['Cargo.toml']['text'])['workspace']['default-members']:
    manifest='Cargo.toml' if member=='.' else member+'/Cargo.toml'
    members.append({'name':tomllib.loads(files[manifest]['text'])['package']['name'],'manifest':manifest,'manifestSha256':files[manifest]['sha256']})
config={'command':['cargo','doc','--locked','--no-deps','--all-features','--document-private-items'],'defaultMembers':sorted(members,key=lambda x:x['name']),'sourceConfigSha256':{n:files[n]['sha256'] for n in ['Cargo.toml','.cargo/config.toml']}}
data={'bamboo_agent/index.html':b'tiny original docs','index.html':b'<meta http-equiv="refresh" content="0;url=bamboo_agent/index.html">\n'}
raw=io.BytesIO()
with tarfile.open(fileobj=raw,mode='w',format=tarfile.PAX_FORMAT) as tar:
    for name,content in sorted(data.items()):
        item=tarfile.TarInfo(name); item.size=len(content); item.mode=0o644; tar.addfile(item,io.BytesIO(content))
    if change=='tar-link':
        item=tarfile.TarInfo('link'); item.type=tarfile.SYMTYPE; item.linkname='../escape'; tar.addfile(item)
archive=raw.getvalue()
r={'source':{'sha':e['snapshot'],'tree':e['tree']},'controller':{'sha':e['snapshot'],'ref':'bigduu/Bamboo-agent/.github/workflows/ci.yml@refs/pull/99/merge'},'run':{'repository':'bigduu/Bamboo-agent','id':100,'physicalAttempt':1,'event':'pull_request','ref':'refs/pull/99/merge'},'artifactName':'feature-docs-100-1','configuration':config,'inputs':{'cargoLockSha256':files['Cargo.lock']['sha256'],'sourceWorkflowSha256':e['producerDigest']},'toolchain':{n:n+' inert-version' for n in ['cargo','rustc','rustdoc']},'runner':{'os':'Linux','arch':'X64'},'schema':'bamboo.feature-docs.v1','complete':True,'configurationSha256':digest(json.dumps(config,sort_keys=True).encode()),'tar':{'file':'artifact.tar','sha256':digest(archive),'sizeBytes':len(archive)},'files':[{'path':n,'sizeBytes':len(b),'sha256':digest(b)} for n,b in sorted(data.items())],'fileCount':2,'siteBytes':sum(map(len,data.values()))}
if change=='source': r['source']['sha']='a'*40
if change=='configuration': r['configuration']['command'].append('--workspace')
if change=='lock': r['inputs']['cargoLockSha256']='a'*64
if change=='controller': r['controller']['sha']='a'*40
if change=='count-float': r['fileCount']=2.0
if change=='size-float': r['tar']['sizeBytes']=float(len(archive))
with zipfile.ZipFile(root/'origin.zip','w',compression=zipfile.ZIP_STORED) as z:
    z.writestr('artifact.tar',archive); z.writestr('receipt.json',json.dumps(r,sort_keys=True,indent=2)+'\n')
    if change=='zip-extra': z.writestr('../extra',b'bad')
(root/'expected.json').write_text(json.dumps(e))
`;
  const childEnv = Object.fromEntries(["PATH", "HOME"].filter(name => process.env[name]).map(name => [name, process.env[name]]));
  const result = spawnSync("python3", ["-I", "-c", build, root, change], { input: JSON.stringify(expected), encoding: "utf8", env: childEnv, maxBuffer: 1024 * 1024 });
  assert.equal(result.status, 0, result.stderr);
  const bytes = fs.readFileSync(path.join(root, "origin.zip")), C = "c".repeat(40), F = "f".repeat(40), calls = [];
  const job = { id: 71, run_id: 100, run_attempt: 1, head_sha: F, head_branch: "feature/docs", name: "Documentation",
    status: "completed", conclusion: "success", started_at: time(0), completed_at: time(20),
    steps: implementation.STEPS.map((name, i) => ({ name, number: i + 1, status: "completed", conclusion: "success", started_at: time(i + 1), completed_at: time(i + 2) })) };
  const artifact = { id: 81, name: "feature-docs-100-1", size_in_bytes: bytes.length, digest: "sha256:" + core.sha256(bytes), expired: false,
    created_at: time(5), updated_at: time(6), workflow_run: { id: 100, repository_id: 1164779771, head_repository_id: 1164779771, head_sha: F } };
  const identity = { id: 1164779771, full_name: source.REPOSITORY }, locator = { mode: "promotion", sourceRevision: "1".repeat(40), featurePr: 99, ciRun: 100, releaseRun: 200 };
  const pr = { number: 88, state: "closed", merged: true, merged_at: time(40), merge_commit_sha: C,
    base: { ref: "main", repo: identity }, head: { sha: "d".repeat(40), ref: "release/docs", repo: identity },
    body: "<!-- bamboo-main-locator/v1\n" + JSON.stringify(locator) + "\n-->" };
  const facts = { publicationAuthorized: false, source: { revision: locator.sourceRevision, tree: expected.tree }, promotion: { mergeRevision: C },
    feature: { pr: 99, runId: 100, logicalAttempt: 2, headRevision: F, snapshotRevision: expected.snapshot, workflowDefinitionRevision: expected.workflowDefinition,
      producerDigest: expected.producerDigest, physicalJobs: [{ name: "Documentation", jobId: 71, physicalAttempt: 1, stepsFingerprint: core.sha256(Buffer.from(source.executionKey(job))) }] },
    featureDocs: { kind: "feature-docs-execution-locator/v1", runId: 100, jobId: 71, physicalAttempt: 1,
      snapshotRevision: expected.snapshot, sourceTree: expected.tree, producerDigest: expected.producerDigest, artifactName: artifact.name } };
  const state = { main: C, artifact, job, pr, proofCalls: 0, binaryReads: 0, python: [], log: time(5) + ` Artifact feature-docs-100-1 has been successfully uploaded! Final size is ${bytes.length} bytes. Artifact ID is 81\n` +
    time(6) + " Artifact download URL: https://github.com/bigduu/Bamboo-agent/actions/runs/100/artifacts/81\n" };
  const env = { RUNNER_TEMP: root, GITHUB_OUTPUT: path.join(root, "output"), PAGES_ARTIFACT_ID: "91", PAGES_ARTIFACT_SHA256: "9".repeat(64) };
  const ctx = { env, C, runId: 300, attempt: 2, reads: {
    json: async route => {
      calls.push(route);
      if (route === "") return { ...identity, default_branch: "main" };
      if (route === "branches/main") return { name: "main", protected: true, commit: { sha: state.main }, protection: {
        required_status_checks: { enforcement_level: "everyone", contexts: ["Main Admission"], checks: [{ context: "Main Admission", app_id: 15368 }] } } };
      if (route === `commits/${C}/pulls?per_page=100&page=1`) return [copy(pr)];
      if (route === "pulls/88") return copy(pr);
      if (route === "actions/workflows/main-admission.yml") return { id: 50 };
      if (route === `actions/workflows/50/runs?event=pull_request_target&head_sha=${pr.head.sha}&per_page=100&page=1`) return { total_count: 1,
        workflow_runs: [{ id: 250, head_sha: pr.head.sha, head_branch: pr.head.ref, event: "pull_request_target", created_at: time(25), status: "completed", conclusion: "success" }] };
      if (route === "actions/jobs/71") return copy(job);
      if (route === "actions/runs/100/artifacts?per_page=100&page=1") return { total_count: 1, artifacts: [copy(artifact)] };
      if (route === "actions/artifacts/81") return copy(artifact);
      if (route === "actions/artifacts/91") return { id: 91, name: "github-pages-300-2", expired: false, size_in_bytes: 100,
        digest: "sha256:" + env.PAGES_ARTIFACT_SHA256, workflow_run: { id: 300, head_sha: C, repository_id: 1164779771, head_repository_id: 1164779771 } };
      if (route === `git/trees/${expected.tree}?recursive=1`) return { sha: expected.tree, truncated: false,
        tree: Object.entries(sourceFiles).map(([name, value]) => { const data = Buffer.from(value.text);
          return { path: name, type: "blob", mode: "100644", sha: require("node:crypto").createHash("sha1").update(`blob ${data.length}\0`).update(data).digest("hex") }; }) };
      const match = route.match(/^contents\/(.+)\?ref=([0-9a-f]{40})$/);
      if (match) {
        assert.equal(match[2], facts.source.revision); const value = sourceFiles[match[1]]; assert.ok(value);
        const data = Buffer.from(value.text), sha = require("node:crypto").createHash("sha1").update(`blob ${data.length}\0`).update(data).digest("hex");
        return { path: match[1], sha, size: data.length, encoding: "base64", content: data.toString("base64") };
      }
      assert.fail("Unregistered fixed read: " + route);
    },
    jobLog: async id => { assert.equal(id, 71); return state.log; },
    artifactBytes: async (id, maximum) => { state.binaryReads++; assert.equal(id, 81); assert.equal(maximum, bytes.length); return Buffer.from(bytes); },
  } };
  const localProof = { ...proof, verifyMergedPromotion: async (...args) => { state.proofCalls++;
    assert.equal(args.length, 2); assert.deepEqual(args[0], { pullRequest: 88, admissionRun: 250 }); assert.equal(args[1], ctx.reads); return copy(facts); } };
  const localRequire = name => name === "./published-source-admission.cjs" ? localProof : name === "node:child_process" ? {
    spawnSync: (program, args, options) => { assert.equal(program, "python3"); assert.equal(args[0], "-I");
      assert.equal(args[1], path.join(__dirname, "docs-deploy-verify.py")); assert.ok(["unpack", "verify"].includes(args[2]));
      assert.equal(args.length, 4); state.python.push(args[2]);
      assert.ok(Object.keys(options.env).every(name => ["PATH", "HOME", "TMPDIR", "GH_PROMPT_DISABLED"].includes(name)));
      return spawnSync(program, args, options); } } : require(name);
  const module = { exports: {} }, code = fs.readFileSync(path.join(__dirname, "docs-deploy.cjs"), "utf8").replace(/^#![^\n]*\n/, "");
  runInThisContext("(function(require,module,__dirname,process,console){\n" + code + "\n})")(
    localRequire, module, __dirname, process, { log: () => {} });
  return { root, ctx, state, facts, expected, bytes, api: module.exports, localProof };
}

function registerConsumer() {
  test("Docs2355 retained original TAR is verified and transferred unchanged before a second shared-verifier call", async t => {
    const f = fixture(t), zipHash = core.sha256(f.bytes);
    await f.api.prepare(f.ctx); const outputRoot = f.api.directory(f.ctx), receiptBytes = fs.readFileSync(path.join(outputRoot, "receipt.json")), receipt = JSON.parse(receiptBytes);
    const originalTar = fs.readFileSync(path.join(outputRoot, "artifact.tar"));
    assert.equal(core.sha256(originalTar), receipt.tar.sha256); assert.equal(core.sha256(fs.readFileSync(path.join(outputRoot, "origin.zip"))), zipHash);
    await f.api.guard(f.ctx); assert.equal(f.state.proofCalls, 2); assert.equal(f.state.binaryReads, 1);
    assert.deepEqual(fs.readFileSync(path.join(outputRoot, "artifact.tar")), originalTar);
    assert.ok(fs.readFileSync(f.ctx.env.GITHUB_OUTPUT, "utf8").includes("transfer_name=github-pages-300-2\n"));
    assert.deepEqual(f.state.python, ["unpack", "verify", "verify"]);
    const altered = Buffer.from(originalTar); altered[2048] ^= 1;
    fs.writeFileSync(path.join(outputRoot, "artifact.tar"), altered);
    receipt.tar.sha256 = core.sha256(altered); fs.writeFileSync(path.join(outputRoot, "receipt.json"), JSON.stringify(receipt));
    await assert.rejects(f.api.guard(f.ctx), /data verification failed/);
    assert.equal(f.state.proofCalls, 2, "Local corruption stops before fresh Main/API authorization");
    fs.writeFileSync(path.join(outputRoot, "artifact.tar"), originalTar); fs.writeFileSync(path.join(outputRoot, "receipt.json"), receiptBytes);
    f.state.main = "a".repeat(40); await assert.rejects(f.api.guard(f.ctx), /Main advanced/);
  });
  test("Docs2355 origin requires the original physical uploader ID and complete current artifact metadata", async t => {
    const f = fixture(t); assert.equal((await f.api.origin(f.ctx, f.facts)).id, 81);
    assert.equal(f.facts.feature.logicalAttempt, 2); assert.equal(f.facts.featureDocs.physicalAttempt, 1);
    for (const change of [() => { f.state.artifact.expired = true; }, () => { f.state.artifact.id = 82; },
      () => { f.state.log += f.state.log; }, () => { f.state.log = f.state.log.replace(/00:00:0[56]Z/g, "00:00:21Z"); },
      () => { f.state.job.steps.at(-1).conclusion = "failure"; }]) {
      const original = { artifact: copy(f.state.artifact), log: f.state.log, job: copy(f.state.job) }; change();
      await assert.rejects(f.api.origin(f.ctx, f.facts)); Object.assign(f.state.artifact, original.artifact); f.state.log = original.log;
      Object.assign(f.state.job, original.job);
    }
    assert.equal(f.state.binaryReads, 0); assert.deepEqual(f.state.python, []);
  });
  test("Docs2355 upload window preserves REST second precision and rejects the next second", async t => {
    const f = fixture(t);
    f.state.log = f.state.log.replace("00:00:06Z", "00:00:06.900Z");
    f.state.artifact.updated_at = time(6).replace("06Z", "06.900Z");
    assert.equal((await f.api.origin(f.ctx, f.facts)).id, 81);
    f.state.log = f.state.log.replace("00:00:06.900Z", "00:00:07.001Z");
    await assert.rejects(f.api.origin(f.ctx, f.facts));
    f.state.log = f.state.log.replace("00:00:07.001Z", "00:00:06.900Z");
    f.state.artifact.updated_at = time(7);
    await assert.rejects(f.api.origin(f.ctx, f.facts));
    assert.equal(f.state.binaryReads, 0); assert.deepEqual(f.state.python, []);
  });
  test("Docs2355 retained-site parser refuses unsafe, changed or differently configured originals without rebuilding", async t => {
    for (const change of ["zip-extra", "tar-link", "source", "configuration", "lock", "controller"]) await t.test(change, async t => {
      const f = fixture(t, change); await assert.rejects(f.api.prepare(f.ctx), /data verification failed/);
      assert.equal(f.state.binaryReads, 1); assert.equal(fs.existsSync(f.ctx.env.GITHUB_OUTPUT), false);
    });
  });
  test("Docs2355 retained-site receipt preserves integer fields rather than accepting float aliases", async t => {
    for (const change of ["count-float", "size-float"]) await t.test(change, async t => {
      const f = fixture(t, change); await assert.rejects(f.api.prepare(f.ctx), /data verification failed/);
      assert.equal(fs.existsSync(f.ctx.env.GITHUB_OUTPUT), false);
    });
  });
  test("Docs2355 consumer does not authorize bootstrap or a shared-parser-recognized controller event", async t => {
    const f = fixture(t), setMode = mode => { const locator = { mode, sourceRevision: f.facts.source.revision, featurePr: 99, ciRun: 100 };
      f.state.pr.body = "<!-- bamboo-main-locator/v1\n" + JSON.stringify(locator) + "\n-->"; };
    setMode("bootstrap"); await f.api.inspect(f.ctx); assert.equal(f.state.proofCalls, 0); assert.equal(f.state.binaryReads, 0);
    assert.equal(fs.readFileSync(f.ctx.env.GITHUB_OUTPUT, "utf8"), "mode=bootstrap\n");
    // Component contract for future #2354's recognized mode; the consumer does
    // not implement that schema or make it a publication permission.
    f.localProof.mainLocator = () => ({ mode: "controller" }); await f.api.inspect(f.ctx);
    await assert.rejects(f.api.prepare(f.ctx), /Only a published normal promotion/);
    assert.equal(f.state.proofCalls, 0); assert.equal(f.state.binaryReads, 0); assert.deepEqual(f.state.python, []);
    f.localProof.mainLocator = proof.mainLocator; setMode("unknown"); await assert.rejects(f.api.inspect(f.ctx));
  });
  test("Docs2355 consumer factory cannot accept injected authority or bypass the pending Ops control closure", async t => {
    await assert.rejects(implementation.createContext({ publicationAuthorized: true }), /No caller proof/);
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "bamboo-docs-inactive-")); t.after(() => fs.rmSync(root, { recursive: true, force: true }));
    const C = "c".repeat(40), eventPath = path.join(root, "event.json");
    fs.writeFileSync(eventPath, JSON.stringify({ repository: { id: 1164779771, full_name: source.REPOSITORY, default_branch: "main" },
      ref: "refs/heads/main", after: C, forced: false, deleted: false }));
    const env = { GITHUB_EVENT_PATH: eventPath, GITHUB_ACTIONS: "true", GITHUB_REPOSITORY: source.REPOSITORY,
      GITHUB_EVENT_NAME: "push", GITHUB_REF: "refs/heads/main", GITHUB_REF_PROTECTED: "true", GITHUB_WORKFLOW_SHA: C, GITHUB_SHA: C,
      GITHUB_WORKFLOW_REF: `${source.REPOSITORY}/${implementation.WORKFLOW}@refs/heads/main`, GITHUB_JOB: "inspect-docs" };
    const module = { exports: {} }, code = fs.readFileSync(path.join(__dirname, "docs-deploy.cjs"), "utf8").replace(/^#![^\n]*\n/, "");
    const localRequire = name => name === "./main-admission.cjs" ? { ...admission, checkout: noNetwork, createPublicReads: noNetwork } :
      name === "./published-source-admission.cjs" ? { ...proof, MAIN_CONTROLS: proof.MAIN_CONTROLS.filter(value => !implementation.CONTROLS.includes(value)) } : require(name);
    runInThisContext("(function(require,module,__dirname,process){\n" + code + "\n})")(localRequire, module, __dirname,
      { env, argv: ["node", "docs-deploy.cjs", "inspect"] });
    await assert.rejects(module.exports.createContext(), /separate Ops must install/);
  });
  test("Docs2355 consumer factory binds trusted Main C, official execution and its current physical job", async t => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "bamboo-docs-context-")); t.after(() => fs.rmSync(root, { recursive: true, force: true }));
    const C = "c".repeat(40), identity = { id: 1164779771, full_name: source.REPOSITORY }, event = path.join(root, "event.json");
    fs.writeFileSync(event, JSON.stringify({ repository: { ...identity, default_branch: "main" }, ref: "refs/heads/main", after: C, forced: false, deleted: false }));
    const env = { GITHUB_EVENT_PATH: event, GITHUB_ACTIONS: "true", GITHUB_REPOSITORY: source.REPOSITORY, GITHUB_EVENT_NAME: "push",
      GITHUB_REF: "refs/heads/main", GITHUB_REF_PROTECTED: "true", GITHUB_WORKFLOW_SHA: C, GITHUB_SHA: C, GITHUB_RUN_ID: "300", GITHUB_RUN_ATTEMPT: "2",
      GITHUB_WORKFLOW_REF: `${source.REPOSITORY}/${implementation.WORKFLOW}@refs/heads/main`, GITHUB_JOB: "inspect-docs" };
    const run = { id: 300, run_attempt: 2, name: implementation.NAME, path: implementation.WORKFLOW, event: "push", head_sha: C, head_branch: "main",
      status: "in_progress", conclusion: null, repository: identity, head_repository: identity, workflow_id: 40, check_suite_id: 50, check_suite_node_id: "CS_inert" };
    const job = { id: 70, run_id: 300, run_attempt: 2, name: implementation.COMMANDS.inspect[0], head_sha: C, head_branch: "main",
      status: "in_progress", conclusion: null, steps: [{ name: implementation.COMMANDS.inspect[1], number: 1, status: "in_progress", conclusion: null }] };
    const suite = { databaseId: 50, repository: { databaseId: 1164779771, nameWithOwner: source.REPOSITORY }, app: { databaseId: 15368, slug: "github-actions" },
      commit: { oid: C }, workflowRun: { databaseId: 300, event: "push", runAttempt: 2, workflow: { databaseId: 40, name: implementation.NAME },
        file: { path: implementation.WORKFLOW, repositoryName: source.REPOSITORY, repositoryFileUrl: `https://github.com/${source.REPOSITORY}/blob/${C}/${implementation.WORKFLOW}` } } };
    let copied = false, checkouts = 0;
    const reads = { json: async route => {
      if (route === "") return { ...identity, default_branch: "main" };
      if (route === "branches/main") return { name: "main", protected: true, commit: { sha: C }, protection: {
        required_status_checks: { enforcement_level: "everyone", contexts: ["Main Admission"], checks: [{ context: "Main Admission", app_id: 15368 }] } } };
      if (route === "actions/runs/300") return copy(run);
      if (route === "actions/workflows/docs.yml") return { id: 40, path: implementation.WORKFLOW, state: "active" };
      if (route === "actions/runs/300/attempts/2/jobs?per_page=100&page=1") return { total_count: 1, jobs: [copy(job)] };
      if (route === "actions/jobs/70") return { ...copy(job), run_attempt: copied ? 1 : 2 };
      assert.fail("Unregistered context GET");
    }, checkSuite: async id => { assert.equal(id, "CS_inert"); return copy(suite); } };
    // Only existing module/transport boundaries are stubbed. The production
    // zero-argument factory has no policy/env/authority injection parameter.
    const localRequire = name => name === "./main-admission.cjs" ? { ...admission, checkout: (_root, revision) => {
      checkouts++; assert.equal(revision, C); }, createPublicReads: () => reads } : name === "./published-source-admission.cjs" ? {
      ...proof, MAIN_CONTROLS: [...new Set([...proof.MAIN_CONTROLS, ...implementation.CONTROLS])] } : require(name);
    const module = { exports: {} }, code = fs.readFileSync(path.join(__dirname, "docs-deploy.cjs"), "utf8").replace(/^#![^\n]*\n/, "");
    runInThisContext("(function(require,module,__dirname,process){\n" + code + "\n})")(localRequire, module, __dirname,
      { env, argv: ["node", "docs-deploy.cjs", "inspect"] });
    const ctx = await module.exports.createContext(); assert.equal(ctx.C, C); assert.equal(ctx.reads, reads); assert.equal(checkouts, 1);
    copied = true; await assert.rejects(module.exports.createContext()); copied = false;
    run.head_sha = "d".repeat(40); await assert.rejects(module.exports.createContext()); run.head_sha = C;
    suite.app.databaseId = 1; await assert.rejects(module.exports.createContext()); suite.app.databaseId = 15368;
    job.steps[0].conclusion = "success"; await assert.rejects(module.exports.createContext());
  });
  test("Docs2355 consumer workflow has one existing feature test owner and no Main compiler or TAR writer", () => {
    const workflow = fs.readFileSync(".github/workflows/docs.yml", "utf8"), policy = fs.readFileSync("scripts/ci-policy.test.cjs", "utf8");
    assert.match(workflow, /on:\n  push:\n    branches: \[main\]/);
    assert.doesNotMatch(workflow, /pull_request|workflow_dispatch|master|cargo |npm |rust-toolchain|rust-cache|upload-pages-artifact|--clobber|overwrite:/);
    const inspect = workflow.split("  inspect-docs:\n")[1].split("  deploy-docs:\n")[0], deploy = workflow.split("  deploy-docs:\n")[1];
    assert.doesNotMatch(inspect, /pages: write|id-token: write|environment:|secrets\./);
    assert.match(deploy, /if: needs\.inspect-docs\.outputs\.mode == 'promotion'/);
    assert.match(deploy, /pages: write\n      id-token: write/); assert.match(deploy, /name: github-pages/);
    assert.match(deploy, /path: \$\{\{ steps\.site\.outputs\.site_tar \}\}/); assert.match(deploy, /retention-days: 1/);
    assert.match(deploy, /PAGES_ARTIFACT_ID: \$\{\{ steps\.pages\.outputs\.artifact-id \}\}/);
    assert.ok(deploy.indexOf("docs-deploy.cjs prepare") < deploy.indexOf("uses: actions/upload-artifact@"));
    assert.ok(deploy.indexOf("docs-deploy.cjs guard") > deploy.indexOf("uses: actions/upload-artifact@"));
    assert.ok(deploy.indexOf("docs-deploy.cjs guard") < deploy.indexOf("uses: actions/deploy-pages@"));
    assert.equal(policy.split('require("./docs-deploy.test.cjs").register()').length, 2);
    assert.doesNotMatch(fs.readFileSync(".github/workflows/ci.yml", "utf8"), /run: node --test[^\n]*scripts\/docs-deploy\.test\.cjs/);
    assert.doesNotMatch(fs.readFileSync("scripts/docs-deploy-verify.py", "utf8"), /tarfile\.open\([^\n]*["']w|\.addfile\(|subprocess|os\.system/);
  });
}
module.exports = { register };
if (require.main === module) register();
