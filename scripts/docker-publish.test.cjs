"use strict";

const assert = require("node:assert/strict");
const { spawnSync } = require("node:child_process");
const { createHash } = require("node:crypto");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const test = require("node:test");

const repository = "bigduu/Bamboo-agent";
const repositoryId = 1164779771;
const version = "2026.11.19";
const source = "a".repeat(40);
const controller = "b".repeat(40);
const dev = "c".repeat(40);
const policyPath = "scripts/sidecar-release.cjs";
const publisherPath = ".github/workflows/publish-crate.yml";
const publisherName = "Publish Crate";
const workflowId = 242748806;
const jobName = "Publish and verify complete executable assets";
const publishStep = "Verify official provenance and publish immutable shared assets";
const publicationScript = fs.readFileSync(path.join(__dirname, "docker-release.py"), "utf8");
const workflow = fs.readFileSync(path.join(__dirname, "../.github/workflows/docker-publish.yml"), "utf8");
const sha256 = bytes => createHash("sha256").update(bytes).digest("hex");

function step(name) {
  const marker = "      - name: " + name + "\n";
  assert.ok(workflow.includes(marker), name);
  return workflow.split(marker)[1].split(/\n      - |\n  [a-z-]+:/)[0];
}

function shell(name) {
  return step(name).split("        run: |\n")[1].trimEnd().split("\n").map(line => line.slice(10)).join("\n");
}

function scratch(t) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "bamboo-docker-test-"));
  t.after(() => fs.rmSync(directory, { force: true, recursive: true }));
  return directory;
}

function policy(contract) {
  return sourceFile(policyPath, "// BAMBOO_SHARED_EXECUTABLES_V" + contract + "\n");
}

function sourceFile(file, content) {
  const bytes = Buffer.from(content);
  return { type: "file", path: file, encoding: "base64", size: bytes.length, content: bytes.toString("base64") };
}

function actionRun(id = 550, attempt = 1, head = controller) {
  return {
    id, workflow_id: workflowId, check_suite_id: id + 1000, run_attempt: attempt, name: publisherName,
    event: "workflow_dispatch", path: publisherPath, head_branch: "dev", head_sha: head,
    repository: { full_name: repository, id: repositoryId }, head_repository: { full_name: repository, id: repositoryId },
    status: "completed", conclusion: "success", created_at: "2026-11-13T00:00:00Z",
    run_started_at: "2026-11-13T00:00:00Z", updated_at: "2026-11-13T00:02:00Z",
  };
}

function actionJob(run, id = run.id + 2000) {
  const steps = [{ name: publishStep, number: 10, started_at: "2026-11-13T00:00:45Z", completed_at: "2026-11-13T00:00:55Z",
    status: "completed", conclusion: "success" }];
  return {
    id, name: jobName, workflow_name: publisherName, run_id: run.id, run_attempt: run.run_attempt,
    head_sha: run.head_sha, head_branch: "dev", status: "completed", conclusion: "success",
    started_at: "2026-11-13T00:00:30Z", completed_at: "2026-11-13T00:01:10Z", steps,
    check_run_url: "https://api.github.com/repos/" + repository + "/check-runs/" + id,
  };
}

function attemptFixture(run, record, job = actionJob(run)) {
  const root = "actions/runs/" + run.id + "/attempts/" + run.run_attempt;
  const logPath = "actions/jobs/" + job.id + "/steps/" + job.steps[0].number + "/logs";
  return {
    routes: {
      [root]: run,
      [root + "/jobs?per_page=100"]: { total_count: 1, jobs: [job] },
      ["check-runs/" + job.id]: {
        id: job.id, name: jobName, app: { id: 15368 }, check_suite: { id: run.check_suite_id },
        head_sha: run.head_sha, details_url: "https://github.com/" + repository + "/actions/runs/" + run.id + "/job/" + job.id,
        status: job.status, conclusion: job.conclusion, started_at: job.started_at, completed_at: job.completed_at,
      },
    },
    logs: { [logPath]: "\ufeff2026-11-13T00:00:55.87523Z " + JSON.stringify(record) + "\n" },
    logPath, job,
  };
}

function releaseFixture(contract) {
  const targets = ["aarch64-apple-darwin", "x86_64-pc-windows-msvc", "x86_64-unknown-linux-gnu"];
  if (contract === 3) targets.splice(1, 0, "aarch64-pc-windows-msvc", "aarch64-unknown-linux-gnu");
  const artifacts = targets.map(target => ({
    target,
    buildMode: "embedded",
    embeddedFrontend: true,
    format: "raw",
    assetName: "bamboo-" + target + (target.endsWith("-windows-msvc") ? ".exe" : ""),
    sha256: sha256(target),
    sizeBytes: 65536,
  }));
  const browserHost = { assetName: "bamboo-browser-host-v1.tar.gz", sha256: sha256("host"), sizeBytes: 1234, files: [] };
  const manifest = { schemaVersion: contract, version, sourceRevision: source, artifacts, browserHost };
  const manifestAsset = { id: 900, name: "bamboo-sidecars-v" + contract + ".json", state: "uploaded" };
  const assets = [...artifacts, browserHost].map((item, index) => ({
    id: index + 1, name: item.assetName, state: "uploaded", size: item.sizeBytes, digest: "sha256:" + item.sha256,
  }));
  assets.push(manifestAsset,
    { id: 901, name: "bamboo-executables-provenance-v" + contract + ".json", state: "uploaded", size: 90 },
    { id: 902, name: "release-provenance.json", state: "uploaded", size: 90 });
  const release = { tag_name: "v" + version, target_commitish: source, draft: false, prerelease: false,
    published_at: "2026-11-13T00:00:20Z", assets };
  const tag = { object: { type: "commit", sha: source } };
  const record = { status: "published-and-verified", version, sourceRevision: source, manifestSha256: sha256(JSON.stringify(manifest)),
    contractVersion: contract, targets };
  return { manifest, manifestAsset, release, tag, policy: policy(contract), record };
}

const mockGitHub = [
  "import json, subprocess, sys",
  "fixture = json.load(sys.stdin)",
  "def read(args, **kwargs):",
  "    assert args[:4] == ['gh', 'api', '--hostname', 'github.com'] and kwargs == {'timeout': 60, 'stderr': subprocess.PIPE}",
  "    route = args[4].split('/', 3)[3]",
  "    if route in fixture['binary']:",
  "        assert args[5:] == ['-H', 'Accept: application/octet-stream']",
  "        return fixture['binary'][route].encode()",
  "    if route.endswith('/logs'):",
  "        assert args[5:] == ['-H', 'X-GitHub-Api-Version: 2026-03-10', '--allow-escape-sequences'] and '/steps/' in route",
  "        return fixture['logs'][route].encode()",
  "    assert len(args) == 5",
  "    return json.dumps(fixture['routes'][route]).encode()",
  "subprocess.check_output = read",
  "exec(compile(fixture['body'], '<Docker release verification step>', 'exec'), {'__name__': '__main__'})",
].join("\n");

function verifyRelease(t, fixture, { eventName = "workflow_dispatch", event = {}, handoff, publisherRun = "550",
  publisherAttempt = "1", customize } = {}) {
  const directory = scratch(t);
  const raw = JSON.stringify(fixture.manifest);
  fixture.manifestAsset.size = Buffer.byteLength(raw);
  fixture.manifestAsset.digest ??= "sha256:" + sha256(raw);
  const routes = {
    ["releases/tags/v" + version]: fixture.release,
    ["git/ref/tags/v" + version]: fixture.tag,
    ["contents/" + policyPath + "?ref=" + source]: fixture.policy,
  };
  const logs = {};
  if (fixture.manifest.schemaVersion === 3) {
    const run = actionRun();
    const original = attemptFixture(run, fixture.record);
    Object.assign(routes, {
      "actions/workflows/publish-crate.yml": { id: workflowId, name: publisherName, path: publisherPath },
      "branches/dev": { name: "dev", protected: true, commit: { sha: dev } },
      ["contents/" + publisherPath + "?ref=" + controller]: sourceFile(publisherPath,
        "# BAMBOO_FROZEN_DEV_PUBLISHER_V1\n# BAMBOO_SHARED_EXECUTABLES_V3\n"),
      "actions/runs/550": run,
      ...original.routes,
    });
    for (const commit of [source, controller]) {
      routes["compare/" + commit + "..." + dev] = { base_commit: { sha: commit }, merge_base_commit: { sha: commit }, status: "ahead" };
    }
    Object.assign(logs, original.logs);
    customize?.({ routes, logs, run, original });
  }
  const output = path.join(directory, "outputs");
  const eventPath = path.join(directory, "event.json");
  fs.writeFileSync(eventPath, JSON.stringify(event));
  if (handoff) {
    fs.mkdirSync(path.join(directory, "release"));
    fs.writeFileSync(path.join(directory, "release/release.json"), JSON.stringify(handoff));
  }
  const result = spawnSync("python3", ["-c", mockGitHub], {
    encoding: "utf8",
    input: JSON.stringify({ body: publicationScript, routes, logs, binary: { "releases/assets/900": raw } }),
    env: { ...process.env, GITHUB_REPOSITORY: repository, GITHUB_EVENT_NAME: eventName, GITHUB_EVENT_PATH: eventPath,
      GITHUB_REF: "refs/heads/dev", GITHUB_REF_PROTECTED: "true", INPUT_VERSION: version, INPUT_SOURCE: source,
      INPUT_PUBLISHER_RUN_ID: publisherRun, INPUT_PUBLISHER_ATTEMPT: publisherAttempt,
      GITHUB_OUTPUT: output, RUNNER_TEMP: directory },
  });
  assert.ifError(result.error);
  if (result.status !== 0) {
    assert.equal(fs.existsSync(output), false);
    return { error: result.stderr };
  }
  return Object.fromEntries(fs.readFileSync(output, "utf8").trim().split("\n").map(line => line.split(/=(.*)/).slice(0, 2)));
}

test("the frozen contract selects each Linux binary by its exact published asset identity", t => {
  for (const contract of [2, 3]) {
    const fixture = releaseFixture(contract);
    const result = verifyRelease(t, fixture, contract === 2 ? { publisherRun: "", publisherAttempt: "" } : {});
    assert.equal(result.contract, String(contract), result.error);
    assert.equal(result.source, source);
    assert.equal(result.version, version);
    const expected = Object.fromEntries(["amd64", "arm64"].map(arch => {
      const target = arch === "amd64" ? "x86_64-unknown-linux-gnu" : "aarch64-unknown-linux-gnu";
      const asset = fixture.release.assets.find(item => item.name === "bamboo-" + target);
      return [arch, asset ? { id: asset.id, size: asset.size, sha256: sha256(target) } : { id: "", size: "", sha256: "" }];
    }));
    assert.deepEqual(JSON.parse(result.linux_assets), expected);
  }
});

test("a v3 release rejects incomplete, mixed or changed originals before emitting image inputs", t => {
  const isArm = value => value.target === "aarch64-unknown-linux-gnu" || value.name === "bamboo-aarch64-unknown-linux-gnu";
  const cases = [
    ["missing ARM64 target", f => { f.manifest.artifacts = f.manifest.artifacts.filter(item => !isArm(item)); }],
    ["duplicate ARM64 target", f => { f.manifest.artifacts.push(f.manifest.artifacts.find(isArm)); }],
    ["missing ARM64 asset", f => { f.release.assets = f.release.assets.filter(item => !isArm(item)); }],
    ["wrong ARM64 hash", f => { f.release.assets.find(isArm).digest = "sha256:" + "0".repeat(64); }],
    ["wrong ARM64 size", f => { f.release.assets.find(isArm).size++; }],
    ["wrong ARM64 asset name", f => { f.manifest.artifacts.find(isArm).assetName = "bamboo-x86_64-unknown-linux-gnu"; }],
    ["duplicate asset ID", f => { f.release.assets.find(isArm).id = f.release.assets[0].id; }],
    ["missing v3 manifest", f => { f.release.assets = f.release.assets.filter(item => item !== f.manifestAsset); }],
    ["changed manifest bytes", f => { f.manifestAsset.digest = "sha256:" + "0".repeat(64); }],
    ["foreign source", f => { f.manifest.sourceRevision = "b".repeat(40); }],
    ["tag drift", f => { f.tag.object.sha = "b".repeat(40); }],
    ["mixed contracts", f => { f.release.assets.push({ ...f.manifestAsset, id: 903, name: "bamboo-sidecars-v2.json" }); }],
  ];
  for (const [name, mutate] of cases) {
    const fixture = releaseFixture(3);
    mutate(fixture);
    assert.match(verifyRelease(t, fixture).error ?? "", /AssertionError/, name);
  }
  const legacy = releaseFixture(2);
  legacy.policy = policy(3);
  assert.match(verifyRelease(t, legacy).error ?? "", /AssertionError/);
});

test("automatic image packaging requires the successful publisher's handoff", t => {
  const event = { workflow_run: actionRun() };
  const handoff = { runId: 550, version, source };
  assert.equal(verifyRelease(t, releaseFixture(3), { eventName: "workflow_run", event, handoff }).contract, "3");
  assert.equal(verifyRelease(t, releaseFixture(2), { eventName: "workflow_run", event, handoff }).contract, "2");
  const failed = verifyRelease(t, releaseFixture(3), { eventName: "workflow_run", event, handoff: { ...handoff, runId: 551 } });
  assert.match(failed.error ?? "", /AssertionError/);
});

test("v3 refuses a replacement manifest even when release asset metadata and all binaries agree", t => {
  const replacement = releaseFixture(3);
  replacement.manifest = { ...replacement.manifest, additional: "coherent replaced bytes" };
  assert.match(verifyRelease(t, replacement).error, /Manifest differs from the original verified v3 publication/);
});

test("v3 authenticates the exact publisher, protected history and completed Actions check and step", t => {
  const cases = [
    ["workflow ID", ({ routes }) => { routes["actions/workflows/publish-crate.yml"].id++; }],
    ["protected Dev", ({ routes }) => { routes["branches/dev"].protected = false; }],
    ["controller ancestry", ({ routes }) => { routes["compare/" + controller + "..." + dev].merge_base_commit.sha = source; }],
    ["controller contract", ({ routes }) => { routes["contents/" + publisherPath + "?ref=" + controller] = sourceFile(publisherPath,
      "# BAMBOO_FROZEN_DEV_PUBLISHER_V1\n# BAMBOO_SHARED_EXECUTABLES_V2\n"); }],
    ["wrong attempt", ({ run }) => { run.run_attempt = 2; }],
    ["wrong repository", ({ run }) => { run.head_repository.id++; }],
    ["wrong head branch", ({ run }) => { run.head_branch = "main"; }],
    ["wrong workflow path", ({ run }) => { run.path = ".github/workflows/ci.yml"; }],
    ["foreign publishing job", ({ original }) => { original.job.head_sha = source; }],
    ["missing step success", ({ original }) => { original.job.steps[0].conclusion = "failure"; }],
    ["wrong Actions app", ({ routes, original }) => { routes["check-runs/" + original.job.id].app.id++; }],
    ["foreign check suite", ({ routes, original }) => { routes["check-runs/" + original.job.id].check_suite.id++; }],
    ["missing proof", ({ logs, original }) => { logs[original.logPath] = "Other log output\n"; }],
    ["ambiguous proof", ({ logs, original }) => { logs[original.logPath] += logs[original.logPath].slice(1); }],
    ["proof from outside the publishing step", ({ logs, original }) => {
      logs[original.logPath] = logs[original.logPath].replace("00:00:55.87523Z", "00:00:56.000Z");
    }],
  ];
  for (const [name, customize] of cases) {
    const outcome = verifyRelease(t, releaseFixture(3), { eventName: "workflow_run", event: { workflow_run: actionRun() },
      handoff: { runId: 550, version, source }, customize });
    assert.match(outcome.error ?? "", /AssertionError/, name);
  }
});

test("automatic v3 can bind a retained finalizer to the original attempt of the same publisher run", t => {
  const fixture = releaseFixture(3);
  const rerun = { ...actionRun(550, 2), run_started_at: "2026-11-13T00:10:00Z", updated_at: "2026-11-13T00:11:00Z" };
  const handoff = { runId: rerun.id, version, source };
  const run = customize => verifyRelease(t, fixture, { eventName: "workflow_run", event: { workflow_run: rerun }, handoff,
    customize: state => {
      const copy = actionJob(rerun, 660);
      const projected = attemptFixture({ ...rerun, created_at: "2026-11-13T00:10:01Z" }, fixture.record, copy);
      Object.assign(state.routes, projected.routes);
      customize?.(state, copy);
      // The projected job has no usable log. Only the physical original does.
    } });
  assert.equal(run().contract, "3");
  assert.match(run((state, copy) => { copy.steps[0].completed_at = "2026-11-13T00:00:56Z"; }).error ?? "",
    /Missing original publishing execution/);
});

test("manual v3 requires both original publisher inputs and reads that exact successful attempt", t => {
  const fixture = releaseFixture(3);
  for (const options of [{ publisherRun: "" }, { publisherAttempt: "" }, { publisherRun: "../foreign" }, { publisherAttempt: "1;2" }]) {
    assert.match(verifyRelease(t, fixture, options).error, /Manual v3 recovery requires publisher_run_id and publisher_attempt/);
  }
  assert.match(verifyRelease(t, fixture, { publisherAttempt: "2" }).error, /Unknown publisher attempt/);
  const newer = verifyRelease(t, fixture, { customize: ({ routes }) => {
    routes["actions/runs/550"] = { ...actionRun(550, 2), status: "in_progress", conclusion: null };
  } });
  assert.equal(newer.contract, "3", newer.error);
  const failed = verifyRelease(t, fixture, { customize: ({ routes }) => {
    routes["actions/runs/550/attempts/1"] = { ...actionRun(), conclusion: "failure" };
  } });
  assert.match(failed.error, /The exact publisher attempt did not succeed/);
  const other = verifyRelease(t, fixture, {
    customize: ({ logs, original }) => {
      logs[original.logPath] = logs[original.logPath].replace(version, "2026.11.20");
    },
  });
  assert.match(other.error, /Publisher verified a different release/);
});

test("the read-only Docker gate loads only the controller's exact verification script", () => {
  const loader = step("Verify release, tag, manifest and original Linux binaries");
  assert.match(loader, /contents\/scripts\/docker-release\.py\?ref=\$\{GITHUB_WORKFLOW_SHA\}/);
  assert.match(loader, /python3 "\$RUNNER_TEMP\/bamboo-docker-release\.py"/);
  assert.match(workflow, /name: Verify completed release\n    permissions:\n      contents: read\n      actions: read\n      checks: read/);
});

test("both v3 architectures package the verified asset, and only v2 ARM64 may build source", () => {
  const published = step("Download and verify the original Linux binary");
  assert.match(published, /if: .*mode == 'build' && \(needs.release.outputs.contract == '3' \|\| matrix.platform == 'linux\/amd64'\)/);
  for (const field of ["id", "sha256", "size"]) {
    assert.ok(published.includes("fromJSON(needs.release.outputs.linux_assets)[matrix.arch]." + field));
  }
  assert.match(published, /contents\/docker\/Dockerfile\.published\?ref=\$\{GITHUB_WORKFLOW_SHA\}/);
  assert.match(published, /sha256sum --check/);
  for (const name of ["Setup Node.js for the historical v2 ARM64 build", "Resolve the historical v2 ARM64 frontend", "Stage the historical v2 ARM64 source"]) {
    assert.match(step(name), /if: .*mode == 'build' && matrix.platform == 'linux\/arm64' && needs.release.outputs.contract == '2'/);
  }
  assert.match(step("Build the candidate once"), /contract == '3' .*bamboo-image\/Dockerfile.*Dockerfile\.arm64/);
  assert.match(step("Select an existing verified candidate"), /docker pull --platform "\$PLATFORM" "\$ref"/);
  assert.ok(step("Verify the packaged executable, loader and embedded frontend").includes("steps.candidate.outputs.ref || steps.candidate.outputs.tag"));
});

function runDockerStep(t, name, { mock, variables = {}, prepare }) {
  const directory = scratch(t);
  const executable = path.join(directory, "docker");
  const output = path.join(directory, "outputs");
  const trace = path.join(directory, "trace");
  fs.writeFileSync(executable, "#!/usr/bin/env bash\nset -eu\nprintf '%s\\n' \"$*\" >> \"$DOCKER_TEST_TRACE\"\n" + mock, { mode: 0o755 });
  prepare?.(directory);
  const result = spawnSync("bash", ["-c", shell(name)], {
    encoding: "utf8",
    env: {
      ...process.env, PATH: directory + path.delimiter + process.env.PATH, RUNNER_TEMP: directory,
      GITHUB_OUTPUT: output, DOCKER_TEST_TRACE: trace, ...variables,
    },
  });
  assert.ifError(result.error);
  return {
    status: result.status, stderr: result.stderr,
    output: fs.existsSync(output) ? fs.readFileSync(output, "utf8") : "",
    trace: fs.existsSync(trace) ? fs.readFileSync(trace, "utf8") : "",
  };
}

test("a same-run retry keeps the retained digest, including when the candidate tag is absent", t => {
  const original = sha256("original");
  const mock = [
    'case "$1 $2" in',
    '  "buildx imagetools")',
    '    [[ "$MOCK_CANDIDATE" != absent ]] || { echo "manifest unknown" >&2; exit 1; }',
    '    printf \'{"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":"sha256:%s"}\\n\' "$MOCK_CANDIDATE" ;;',
    '  "pull --platform") test "$MOCK_PULL" = true ;;',
    '  *) exit 2 ;;',
    'esac',
  ].join("\n");
  const run = (candidate, { pull = true, platformReceipt = "linux/arm64" } = {}) => runDockerStep(t, "Select an existing verified candidate", {
    mock,
    variables: { GITHUB_REPOSITORY_OWNER: "bigduu", RELEASE_VERSION: version, PLATFORM: "linux/arm64",
      RETAINED_DIGEST_ARTIFACT: "99", MOCK_CANDIDATE: candidate, MOCK_PULL: String(pull) },
    prepare: directory => {
      const receipt = path.join(directory, "bamboo-retained-digest");
      fs.mkdirSync(receipt);
      fs.writeFileSync(path.join(receipt, original), platformReceipt + "\n");
    },
  });
  for (const result of [run(original), run("absent")]) {
    assert.equal(result.status, 0, result.stderr);
    assert.match(result.output, /mode=reuse/);
    assert.match(result.trace, new RegExp("pull --platform linux/arm64 ghcr.io/bigduu/bamboo@sha256:" + original));
  }
  const conflict = run(sha256("foreign"));
  assert.equal(conflict.status, 1);
  assert.match(conflict.stderr, /differs from the original retained/);
  assert.doesNotMatch(conflict.trace, /^pull/m);
  assert.equal(run("absent", { pull: false }).status, 1);
  assert.equal(run(original, { platformReceipt: "linux/amd64" }).status, 1);
  assert.doesNotMatch(workflow, /overwrite: true/);
});

test("same-run digest selection accepts one authentic retained artifact and rejects unknown state", t => {
  const artifact = { id: 99, name: "digest-arm64", expired: false, workflow_run: { id: 102 } };
  const run = (total, records) => runDockerStep(t, "Find a retained image digest", {
    mock: "exit 2",
    variables: { GITHUB_REPOSITORY: repository, GITHUB_RUN_ID: "102", ARTIFACT_NAME: artifact.name,
      MOCK_ARTIFACTS: JSON.stringify({ total_count: total, artifacts: records }) },
    prepare: directory => {
      fs.writeFileSync(path.join(directory, "gh"), [
        "#!/usr/bin/env bash", "set -eu",
        'test "$*" = "api --hostname github.com repos/bigduu/Bamboo-agent/actions/runs/102/artifacts?name=digest-arm64&per_page=100"',
        'echo "$MOCK_ARTIFACTS"',
      ].join("\n"), { mode: 0o755 });
    },
  });
  assert.equal(run(0, []).output, "id=\n");
  assert.equal(run(1, [artifact]).output, "id=99\n");
  for (const result of [run(1, []), run(2, [artifact, artifact]), run(1, [{ ...artifact, expired: true }]),
    run(1, [{ ...artifact, workflow_run: { id: 101 } }])]) {
    assert.equal(result.output, "");
    assert.notEqual(result.status, 0);
  }
});

test("the image smoke checks the pinned executable and real frontend path before accepting an image", t => {
  const mock = [
    'case "$1 $2" in',
    '  "image inspect")',
    '    case "$4" in',
    '      "{{.Os}}/{{.Architecture}}") echo "$MOCK_PLATFORM" ;;',
    '      *revision*) echo "$RELEASE_SOURCE" ;;',
    '      *version*) echo "$RELEASE_VERSION" ;;',
    '      *) exit 2 ;;',
    '    esac ;;',
    '  "run --platform")',
    '    test "$3" = "$PLATFORM"',
    '    if [ "$4" = "--network" ]; then test "$5 $6" = "none -d"; echo container-id',
    '    elif [ "$5" = "--entrypoint" ]; then echo "$MOCK_SHA  /usr/local/bin/bamboo"',
    '    else echo "bamboo $RELEASE_VERSION"; fi ;;',
    '  "exec container-id")',
    '    grep -Fq "http://127.0.0.1:9562/api/v1/health" <<< "$5"',
    '    grep -Fq "http://127.0.0.1:9562/)" <<< "$5"',
    '    grep -Fq "/data/frontend/.frontend-manifest.json" <<< "$5"',
    '    test "$MOCK_HEALTH" = true ;;',
    '  "inspect --format") echo false ;;',
    '  "logs container-id"|"rm -f") : ;;',
    '  *) exit 2 ;;',
    'esac',
  ].join("\n");
  const run = (arch, overrides = {}) => runDockerStep(t, "Verify the packaged executable, loader and embedded frontend", {
    mock,
    variables: { IMAGE_REF: "ghcr.io/bigduu/bamboo@sha256:" + "f".repeat(64), RELEASE_VERSION: version, RELEASE_SOURCE: source,
      CONTRACT: "3", PLATFORM: "linux/" + arch, EXPECTED_SHA256: sha256(arch), MOCK_PLATFORM: "linux/" + arch,
      MOCK_SHA: sha256(arch), MOCK_HEALTH: "true", ...overrides },
  });
  for (const arch of ["amd64", "arm64"]) {
    const result = run(arch);
    assert.equal(result.status, 0, result.stderr || result.trace);
    assert.match(result.trace, /run --platform linux\/[a-z0-9]+ --network none -d /);
    assert.match(result.trace, /rm -f container-id/);
  }
  for (const overrides of [{ MOCK_SHA: sha256("amd64") }, { MOCK_PLATFORM: "linux/amd64" }]) {
    const failed = run("arm64", overrides);
    assert.equal(failed.status, 1);
    assert.doesNotMatch(failed.trace, /exec container-id/);
  }
  const unhealthy = run("arm64", { MOCK_HEALTH: "false" });
  assert.equal(unhealthy.status, 1);
  assert.match(unhealthy.trace, /logs container-id/);
  assert.match(unhealthy.trace, /rm -f container-id/);
  const historical = run("arm64", { CONTRACT: "2", EXPECTED_SHA256: "" });
  assert.equal(historical.status, 0, historical.stderr);
  assert.doesNotMatch(historical.trace, /exec container-id|--entrypoint/);
});

test("the index binds the verified digest to the matching architecture and updates aliases only afterward", t => {
  const amd64 = sha256("linux/amd64");
  const arm64 = sha256("linux/arm64");
  const index = sha256("index");
  const mock = [
    'case "$1 $2 $3 $4" in',
    '  "buildx imagetools inspect --raw") echo "$MOCK_MANIFEST" ;;',
    '  "buildx imagetools inspect --format") printf \'{"mediaType":"application/vnd.oci.image.index.v1+json","digest":"sha256:%s"}\\n\' "$MOCK_INDEX" ;;',
    '  "buildx imagetools create -t"|"buildx imagetools inspect $IMAGE_TAG") : ;;',
    '  *) exit 2 ;;',
    'esac',
  ].join("\n");
  const imageTag = "ghcr.io/bigduu/bamboo:v" + version;
  const run = (digests, { includeArm = true, armContent = "linux/arm64", latest = "v" + version } = {}) => {
    const manifest = { manifests: ["amd64", "arm64"].map((architecture, i) => ({
      digest: "sha256:" + digests[i], platform: { os: "linux", architecture },
    })) };
    return runDockerStep(t, "Publish the versioned manifest", {
      mock,
      variables: { GITHUB_REPOSITORY_OWNER: "bigduu", GITHUB_REPOSITORY: repository, RELEASE_VERSION: version,
        RELEASE_SOURCE: source, MOCK_MANIFEST: JSON.stringify(manifest), MOCK_INDEX: index, IMAGE_TAG: imageTag },
      prepare: directory => {
        for (const [arch, digest, value] of [["amd64", amd64, "linux/amd64"], ["arm64", arm64, armContent]]) {
          if (arch === "arm64" && !includeArm) continue;
          const artifact = path.join(directory, "bamboo-digests/digest-" + arch);
          fs.mkdirSync(artifact, { recursive: true });
          fs.writeFileSync(path.join(artifact, digest), value + "\n");
        }
        fs.writeFileSync(path.join(directory, "gh"), "#!/usr/bin/env bash\nprintf '%s\\n' " + latest + "\n", { mode: 0o755 });
      },
    });
  };
  const matching = run([amd64, arm64]);
  assert.equal(matching.status, 0, matching.stderr);
  assert.equal(matching.output, "image=ghcr.io/bigduu/bamboo@sha256:" + index + "\n");
  assert.match(matching.trace, new RegExp("create -t .*:sha-aaaaaaa -t .*:latest .*@" + "sha256:" + index));
  assert.doesNotMatch(run([amd64, arm64], { latest: "v2026.11.20" }).trace, /:latest/);
  for (const failed of [run([arm64, amd64]), run([amd64, arm64], { includeArm: false }),
    run([amd64, arm64], { armContent: "linux/amd64" })]) {
    assert.equal(failed.status, 1);
    assert.doesNotMatch(failed.trace, /imagetools create/);
  }
});
