const assert = require("node:assert/strict");
const crypto = require("node:crypto");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { spawnSync } = require("node:child_process");
const test = require("node:test");

const {
  calculateResourcesSha256,
  verifyLotusNextArtifact,
} = require("./lotus-next-artifact.cjs");

function sha256(value) {
  return crypto.createHash("sha256").update(value).digest("hex");
}

function writeArtifactFixture(directory) {
  fs.mkdirSync(path.join(directory, "assets"), { recursive: true });
  fs.writeFileSync(
    path.join(directory, "index.html"),
    "<main>Lotus Next</main>\n",
  );
  fs.writeFileSync(
    path.join(directory, "assets", "app.js"),
    "console.log('ok')\n",
  );

  const resources = [
    {
      path: "assets/app.js",
      size: fs.statSync(path.join(directory, "assets", "app.js")).size,
      sha256: sha256(fs.readFileSync(path.join(directory, "assets", "app.js"))),
    },
    {
      path: "index.html",
      size: fs.statSync(path.join(directory, "index.html")).size,
      sha256: sha256(fs.readFileSync(path.join(directory, "index.html"))),
    },
  ];
  const manifest = {
    schemaVersion: 1,
    packageName: "@bigduu/lotus-next",
    packageVersion: "2026.9.14",
    sourceRevision: "a".repeat(40),
    sourceDirty: false,
    entrypoint: "index.html",
    resourcesSha256: calculateResourcesSha256(resources),
    resources,
  };
  fs.writeFileSync(
    path.join(directory, "lotus-next-manifest.json"),
    `${JSON.stringify(manifest, null, 2)}\n`,
  );
  return { directory, manifest };
}

function artifactFixture() {
  const directory = fs.mkdtempSync(
    path.join(os.tmpdir(), "bamboo-lotus-next-artifact-"),
  );
  return writeArtifactFixture(directory);
}

test("verifies the exact clean Lotus Next artifact identity and resources", (t) => {
  const fixture = artifactFixture();
  t.after(() => fs.rmSync(fixture.directory, { recursive: true, force: true }));

  const result = verifyLotusNextArtifact({
    distDirectory: fixture.directory,
    expectedIdentity: {
      packageName: fixture.manifest.packageName,
      packageVersion: fixture.manifest.packageVersion,
      sourceRevision: fixture.manifest.sourceRevision,
      sourceDirty: false,
      entrypoint: fixture.manifest.entrypoint,
      resourcesSha256: fixture.manifest.resourcesSha256,
    },
  });

  assert.equal(result.manifest.resources.length, 2);
  assert.match(result.manifestSha256, /^[0-9a-f]{64}$/);
});

test("rejects a resource whose bytes changed after the manifest was written", (t) => {
  const fixture = artifactFixture();
  t.after(() => fs.rmSync(fixture.directory, { recursive: true, force: true }));
  fs.appendFileSync(
    path.join(fixture.directory, "assets", "app.js"),
    "tampered\n",
  );

  assert.throws(
    () => verifyLotusNextArtifact({ distDirectory: fixture.directory }),
    /does not match its manifest/,
  );
});

test("rejects an unlisted resource and a mismatched source identity", (t) => {
  const fixture = artifactFixture();
  t.after(() => fs.rmSync(fixture.directory, { recursive: true, force: true }));
  fs.writeFileSync(
    path.join(fixture.directory, "unlisted.txt"),
    "unexpected\n",
  );

  assert.throws(
    () => verifyLotusNextArtifact({ distDirectory: fixture.directory }),
    /resource inventory does not match/,
  );

  fs.rmSync(path.join(fixture.directory, "unlisted.txt"));
  assert.throws(
    () =>
      verifyLotusNextArtifact({
        distDirectory: fixture.directory,
        expectedIdentity: { sourceRevision: "b".repeat(40) },
      }),
    /sourceRevision .* does not match expected/,
  );
});

test("allows an explicitly ignored Bamboo wrapper manifest only", (t) => {
  const fixture = artifactFixture();
  t.after(() => fs.rmSync(fixture.directory, { recursive: true, force: true }));
  fs.writeFileSync(
    path.join(fixture.directory, "frontend-manifest.json"),
    "{}\n",
  );

  assert.doesNotThrow(() =>
    verifyLotusNextArtifact({
      distDirectory: fixture.directory,
      ignoredPaths: ["frontend-manifest.json"],
    }),
  );
});

test("a rejected pinned input leaves the last committed package byte-for-byte intact", (t) => {
  const root = fs.mkdtempSync(
    path.join(os.tmpdir(), "bamboo-frontend-stage-test-"),
  );
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));

  const scriptsDirectory = path.join(root, "scripts");
  const packageRoot = path.join(root, "node_modules", "@bigduu", "lotus-next");
  const serverRoot = path.join(root, "crates", "app", "bamboo-server");
  fs.mkdirSync(scriptsDirectory, { recursive: true });
  fs.mkdirSync(packageRoot, { recursive: true });
  fs.mkdirSync(serverRoot, { recursive: true });
  fs.copyFileSync(
    path.join(__dirname, "frontend-package.cjs"),
    path.join(scriptsDirectory, "frontend-package.cjs"),
  );
  fs.copyFileSync(
    path.join(__dirname, "lotus-next-artifact.cjs"),
    path.join(scriptsDirectory, "lotus-next-artifact.cjs"),
  );

  const fixture = writeArtifactFixture(path.join(packageRoot, "dist"));
  fs.writeFileSync(
    path.join(packageRoot, "package.json"),
    `${JSON.stringify(
      {
        name: "@bigduu/lotus-next",
        version: fixture.manifest.packageVersion,
      },
      null,
      2,
    )}\n`,
  );
  const manifestSource = fs.readFileSync(
    path.join(packageRoot, "dist", "lotus-next-manifest.json"),
    "utf8",
  );
  const lock = {
    schemaVersion: 1,
    packageName: fixture.manifest.packageName,
    packageVersion: fixture.manifest.packageVersion,
    sourceRevision: fixture.manifest.sourceRevision,
    sourceDirty: false,
    entrypoint: fixture.manifest.entrypoint,
    resourcesSha256: fixture.manifest.resourcesSha256,
    manifestSha256: sha256(manifestSource),
  };
  fs.writeFileSync(
    path.join(scriptsDirectory, "frontend-package-lock.json"),
    `${JSON.stringify(lock, null, 2)}\n`,
  );

  const stage = () =>
    spawnSync(
      process.execPath,
      [path.join(scriptsDirectory, "frontend-package.cjs"), "stage"],
      {
        cwd: root,
        encoding: "utf8",
        env: {
          ...process.env,
          LOTUS_PACKAGE_NAME: "@bigduu/lotus-next",
          LOTUS_SOURCE: "package",
        },
      },
    );

  const first = stage();
  assert.equal(first.status, 0, `${first.stdout}\n${first.stderr}`);
  const committedDirectory = path.join(serverRoot, "frontend_package");
  const zipBefore = fs.readFileSync(
    path.join(committedDirectory, "lotus-frontend.zip"),
  );
  const manifestBefore = fs.readFileSync(
    path.join(committedDirectory, "frontend-manifest.json"),
  );

  fs.appendFileSync(path.join(packageRoot, "dist", "index.html"), "tampered\n");
  const rejected = stage();
  assert.equal(rejected.status, 1, `${rejected.stdout}\n${rejected.stderr}`);
  assert.match(rejected.stderr, /does not match its manifest/);
  assert.deepEqual(
    fs.readFileSync(path.join(committedDirectory, "lotus-frontend.zip")),
    zipBefore,
  );
  assert.deepEqual(
    fs.readFileSync(path.join(committedDirectory, "frontend-manifest.json")),
    manifestBefore,
  );
});
