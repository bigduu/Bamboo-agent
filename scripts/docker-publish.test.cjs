"use strict";

const assert = require("node:assert/strict");
const { spawnSync } = require("node:child_process");
const { createHash } = require("node:crypto");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const test = require("node:test");

const repository = "bigduu/Bamboo-agent";
const version = "2026.11.19";
const source = "a".repeat(40);
const policyPath = "scripts/sidecar-release.cjs";
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
  const bytes = Buffer.from("// BAMBOO_SHARED_EXECUTABLES_V" + contract + "\n");
  return { type: "file", path: policyPath, encoding: "base64", size: bytes.length, content: bytes.toString("base64") };
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
  const release = { tag_name: "v" + version, target_commitish: source, draft: false, prerelease: false, published_at: "2026-11-13", assets };
  const tag = { object: { type: "commit", sha: source } };
  return { manifest, manifestAsset, release, tag, policy: policy(contract) };
}

const mockGitHub = [
  "import json, subprocess, sys",
  "fixture = json.load(sys.stdin)",
  "def read(args, **kwargs):",
  "    assert args[:4] == ['gh', 'api', '--hostname', 'github.com'] and kwargs == {'timeout': 60}",
  "    route = args[4].split('/', 3)[3]",
  "    if route in fixture['binary']:",
  "        assert args[5:] == ['-H', 'Accept: application/octet-stream']",
  "        return fixture['binary'][route].encode()",
  "    assert len(args) == 5",
  "    return json.dumps(fixture['routes'][route]).encode()",
  "subprocess.check_output = read",
  "exec(compile(fixture['body'], '<Docker release verification step>', 'exec'))",
].join("\n");

function verifyRelease(t, fixture, { eventName = "workflow_dispatch", event = {}, handoff } = {}) {
  const directory = scratch(t);
  const raw = JSON.stringify(fixture.manifest);
  fixture.manifestAsset.size = Buffer.byteLength(raw);
  fixture.manifestAsset.digest ??= "sha256:" + sha256(raw);
  const routes = {
    ["releases/tags/v" + version]: fixture.release,
    ["git/ref/tags/v" + version]: fixture.tag,
    ["contents/" + policyPath + "?ref=" + source]: fixture.policy,
  };
  const output = path.join(directory, "outputs");
  const eventPath = path.join(directory, "event.json");
  fs.writeFileSync(eventPath, JSON.stringify(event));
  if (handoff) {
    fs.mkdirSync(path.join(directory, "release"));
    fs.writeFileSync(path.join(directory, "release/release.json"), JSON.stringify(handoff));
  }
  const body = shell("Verify release, tag, manifest and original Linux binaries").split("python3 - <<'PY'\n")[1].split("\nPY")[0];
  const result = spawnSync("python3", ["-c", mockGitHub], {
    encoding: "utf8",
    input: JSON.stringify({ body, routes, binary: { "releases/assets/900": raw } }),
    env: { ...process.env, GITHUB_REPOSITORY: repository, GITHUB_EVENT_NAME: eventName, GITHUB_EVENT_PATH: eventPath,
      GITHUB_REF: "refs/heads/dev", GITHUB_REF_PROTECTED: "true", INPUT_VERSION: version, INPUT_SOURCE: source,
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
    const result = verifyRelease(t, fixture);
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
  const event = { workflow_run: {
    id: 550, event: "workflow_dispatch", conclusion: "success", path: ".github/workflows/publish-crate.yml",
    head_branch: "dev", repository: { full_name: repository }, head_repository: { full_name: repository },
  } };
  const handoff = { runId: 550, version, source };
  assert.equal(verifyRelease(t, releaseFixture(3), { eventName: "workflow_run", event, handoff }).contract, "3");
  const failed = verifyRelease(t, releaseFixture(3), { eventName: "workflow_run", event, handoff: { ...handoff, runId: 551 } });
  assert.match(failed.error ?? "", /AssertionError/);
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
  assert.equal(matching.output, "image=" + imageTag + "\n");
  assert.match(matching.trace, new RegExp("create -t .*:sha-aaaaaaa -t .*:latest .*@" + "sha256:" + index));
  assert.doesNotMatch(run([amd64, arm64], { latest: "v2026.11.20" }).trace, /:latest/);
  for (const failed of [run([arm64, amd64]), run([amd64, arm64], { includeArm: false }),
    run([amd64, arm64], { armContent: "linux/amd64" })]) {
    assert.equal(failed.status, 1);
    assert.doesNotMatch(failed.trace, /imagetools create/);
  }
});
