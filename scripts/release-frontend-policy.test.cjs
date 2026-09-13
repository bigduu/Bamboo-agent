"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { spawnSync } = require("node:child_process");
const test = require("node:test");

const {
  FROM_LOCK,
  LEGACY_LOTUS_PACKAGE_NAME,
  LEGACY_LOTUS_VERSION,
  formatGithubOutputs,
  resolveReleaseFrontend,
} = require("./release-frontend-policy.cjs");

const ROOT = path.resolve(__dirname, "..");
const LOTUS_NEXT_PACKAGE_NAME = "@bigduu/lotus-next";
const LOTUS_NEXT_VERSION = "2026.9.14";

test("defaults releases and tag events to the exact locked Lotus Next artifact", () => {
  assert.deepEqual(resolveReleaseFrontend(), {
    packageName: LOTUS_NEXT_PACKAGE_NAME,
    packageVersion: LOTUS_NEXT_VERSION,
    frontendName: "lotus-next",
  });
  assert.deepEqual(
    resolveReleaseFrontend({ packageName: "", requestedVersion: "" }),
    resolveReleaseFrontend({
      packageName: LOTUS_NEXT_PACKAGE_NAME,
      requestedVersion: FROM_LOCK,
    }),
  );
});

test("accepts only the locked Lotus Next version", () => {
  assert.equal(
    resolveReleaseFrontend({
      packageName: LOTUS_NEXT_PACKAGE_NAME,
      requestedVersion: LOTUS_NEXT_VERSION,
    }).packageVersion,
    LOTUS_NEXT_VERSION,
  );
  for (const requestedVersion of ["latest", "2026.9.13", "2026.9.15"]) {
    assert.throws(
      () =>
        resolveReleaseFrontend({
          packageName: LOTUS_NEXT_PACKAGE_NAME,
          requestedVersion,
        }),
      /does not match the allowed .* version/,
    );
  }
});

test("the explicit legacy rollback always resolves to one fixed version", () => {
  for (const requestedVersion of [
    undefined,
    "",
    FROM_LOCK,
    LEGACY_LOTUS_VERSION,
  ]) {
    assert.deepEqual(
      resolveReleaseFrontend({
        packageName: LEGACY_LOTUS_PACKAGE_NAME,
        requestedVersion,
      }),
      {
        packageName: LEGACY_LOTUS_PACKAGE_NAME,
        packageVersion: LEGACY_LOTUS_VERSION,
        frontendName: "lotus",
      },
    );
  }
  assert.throws(
    () =>
      resolveReleaseFrontend({
        packageName: LEGACY_LOTUS_PACKAGE_NAME,
        requestedVersion: "latest",
      }),
    /does not match the allowed .* version/,
  );
});

test("rejects unsupported package names before installation", () => {
  assert.throws(
    () =>
      resolveReleaseFrontend({
        packageName: "@bigduu/not-a-frontend",
        requestedVersion: FROM_LOCK,
      }),
    /Unsupported release frontend package/,
  );
});

test("GitHub outputs are single-line fixed fields", () => {
  const resolution = resolveReleaseFrontend();
  assert.equal(
    formatGithubOutputs(resolution),
    [
      `package_name=${LOTUS_NEXT_PACKAGE_NAME}`,
      `package_version=${LOTUS_NEXT_VERSION}`,
      "frontend_name=lotus-next",
      "",
    ].join("\n"),
  );
  assert.throws(
    () =>
      formatGithubOutputs({
        ...resolution,
        packageVersion: `${LOTUS_NEXT_VERSION}\nunsafe=value`,
      }),
    /must not contain a line break/,
  );
});

test("CLI appends the exact resolution to GITHUB_OUTPUT", (t) => {
  const directory = fs.mkdtempSync(
    path.join(os.tmpdir(), "bamboo-release-frontend-policy-"),
  );
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const outputPath = path.join(directory, "github-output");
  const result = spawnSync(
    process.execPath,
    [path.join(__dirname, "release-frontend-policy.cjs")],
    {
      cwd: ROOT,
      encoding: "utf8",
      env: {
        ...process.env,
        FRONTEND_PACKAGE: LOTUS_NEXT_PACKAGE_NAME,
        FRONTEND_VERSION: LOTUS_NEXT_VERSION,
        GITHUB_OUTPUT: outputPath,
      },
    },
  );

  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
  assert.equal(
    fs.readFileSync(outputPath, "utf8"),
    formatGithubOutputs(resolveReleaseFrontend()),
  );
});

test("crate and Docker publishers share the fail-closed resolver contract", () => {
  const workflowPaths = [
    ".github/workflows/publish-crate.yml",
    ".github/workflows/docker-publish.yml",
  ];
  for (const relativePath of workflowPaths) {
    const source = fs.readFileSync(path.join(ROOT, relativePath), "utf8");
    assert.match(source, /frontend_package:\n/);
    assert.match(source, /default: "@bigduu\/lotus-next"/);
    assert.match(source, /- "@bigduu\/lotus-next"/);
    assert.match(source, /- "@bigduu\/lotus"/);
    assert.match(source, /default: "from_lock"/);
    assert.match(source, /node scripts\/release-frontend-policy\.cjs/);
    assert.match(
      source,
      /LOTUS_PACKAGE_NAME: \$\{\{ steps\.frontend\.outputs\.package_name \}\}/,
    );
    assert.match(
      source,
      /LOTUS_VERSION: \$\{\{ steps\.frontend\.outputs\.package_version \}\}/,
    );
    assert.match(
      source,
      /EXPECTED_FRONTEND_NAME: \$\{\{ steps\.frontend\.outputs\.frontend_name \}\}/,
    );
    assert.doesNotMatch(source, /LOTUS_PACKAGE_NAME: "@bigduu\/lotus"/);
    assert.doesNotMatch(source, /@bigduu\/lotus@\$\{LOTUS_VERSION\}/);
    assert.doesNotMatch(source, /lotus_version \|\| 'latest'/);
  }

  const ci = fs.readFileSync(
    path.join(ROOT, ".github/workflows/ci.yml"),
    "utf8",
  );
  assert.equal(
    ci.match(/scripts\/release-frontend-policy\.test\.cjs/g)?.length,
    2,
  );
});
