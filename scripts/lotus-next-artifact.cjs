const crypto = require("node:crypto");
const fs = require("node:fs");
const path = require("node:path");

const ARTIFACT_MANIFEST_FILE = "lotus-next-manifest.json";
const ARTIFACT_MANIFEST_SCHEMA_VERSION = 1;
const LOTUS_NEXT_PACKAGE_NAME = "@bigduu/lotus-next";

const MANIFEST_KEYS = [
  "schemaVersion",
  "packageName",
  "packageVersion",
  "sourceRevision",
  "sourceDirty",
  "entrypoint",
  "resourcesSha256",
  "resources",
];
const RESOURCE_KEYS = ["path", "size", "sha256"];
const LOCK_KEYS = [
  "schemaVersion",
  "packageName",
  "packageVersion",
  "sourceRevision",
  "sourceDirty",
  "entrypoint",
  "resourcesSha256",
  "manifestSha256",
];
const STRICT_SEMVER =
  /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*)(?:\.(?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*))*))?(?:\+([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?$/;
const REVISION_PATTERN = /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/;
const SHA256_PATTERN = /^[0-9a-f]{64}$/;
const CONTROL_CHARACTER_PATTERN = /\p{Cc}/u;

function sha256(value) {
  return crypto.createHash("sha256").update(value).digest("hex");
}

function assertExactKeys(value, expected, label) {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(`${label} must be an object`);
  }
  const actual = Object.keys(value);
  if (
    actual.length !== expected.length ||
    actual.some((key, index) => key !== expected[index])
  ) {
    throw new Error(`${label} must contain exactly: ${expected.join(", ")}`);
  }
}

function assertPackageVersion(version) {
  if (typeof version !== "string" || !STRICT_SEMVER.test(version)) {
    throw new Error(
      `invalid Lotus Next package version: ${JSON.stringify(version)}`,
    );
  }
}

function assertSourceRevision(revision) {
  if (typeof revision !== "string" || !REVISION_PATTERN.test(revision)) {
    throw new Error(
      "Lotus Next source revision must be a lowercase Git object ID",
    );
  }
}

function assertSha256(value, label) {
  if (typeof value !== "string" || !SHA256_PATTERN.test(value)) {
    throw new Error(`${label} must be a lowercase SHA-256 digest`);
  }
}

function assertResourcePath(resourcePath) {
  if (
    typeof resourcePath !== "string" ||
    resourcePath.length === 0 ||
    resourcePath !== resourcePath.normalize("NFC") ||
    path.posix.isAbsolute(resourcePath) ||
    path.win32.isAbsolute(resourcePath) ||
    resourcePath.includes("\\") ||
    CONTROL_CHARACTER_PATTERN.test(resourcePath)
  ) {
    throw new Error(
      `unsafe Lotus Next resource path: ${JSON.stringify(resourcePath)}`,
    );
  }

  const segments = resourcePath.split("/");
  if (
    segments.some(
      (segment) => segment.length === 0 || segment === "." || segment === "..",
    ) ||
    path.posix.normalize(resourcePath) !== resourcePath ||
    resourcePath === ARTIFACT_MANIFEST_FILE
  ) {
    throw new Error(
      `unsafe Lotus Next resource path: ${JSON.stringify(resourcePath)}`,
    );
  }
}

function assertRegularFile(filePath, label) {
  const metadata = fs.lstatSync(filePath);
  if (metadata.isSymbolicLink() || !metadata.isFile()) {
    throw new Error(`${label} must be a regular file and not a symbolic link`);
  }
  return metadata;
}

function listResourcePaths(distDirectory, ignoredPaths = []) {
  const rootMetadata = fs.lstatSync(distDirectory);
  if (rootMetadata.isSymbolicLink() || !rootMetadata.isDirectory()) {
    throw new Error(
      "Lotus Next dist root must be a directory and not a symbolic link",
    );
  }

  const ignored = new Set([ARTIFACT_MANIFEST_FILE, ...ignoredPaths]);
  const resources = [];
  const visit = (directory, prefix) => {
    const entries = fs
      .readdirSync(directory, { withFileTypes: true })
      .sort((left, right) => left.name.localeCompare(right.name));
    for (const entry of entries) {
      const relativePath = prefix ? `${prefix}/${entry.name}` : entry.name;
      const absolutePath = path.join(directory, entry.name);
      if (ignored.has(relativePath)) continue;
      assertResourcePath(relativePath);
      if (entry.isSymbolicLink()) {
        throw new Error(
          `Lotus Next resource ${relativePath} must not be a symbolic link`,
        );
      }
      if (entry.isDirectory()) {
        visit(absolutePath, relativePath);
      } else if (entry.isFile()) {
        resources.push(relativePath);
      } else {
        throw new Error(
          `Lotus Next resource ${relativePath} is not a regular file`,
        );
      }
    }
  };

  visit(distDirectory, "");
  return resources.sort();
}

function resourceRecord(distDirectory, resourcePath) {
  assertResourcePath(resourcePath);
  const absolutePath = path.join(distDirectory, ...resourcePath.split("/"));
  const metadata = assertRegularFile(
    absolutePath,
    `Lotus Next resource ${resourcePath}`,
  );
  const contents = fs.readFileSync(absolutePath);
  if (metadata.size !== contents.byteLength) {
    throw new Error(
      `Lotus Next resource ${resourcePath} changed while being read`,
    );
  }
  return {
    path: resourcePath,
    size: contents.byteLength,
    sha256: sha256(contents),
  };
}

function calculateResourcesSha256(resources) {
  return sha256(
    resources
      .map(
        (resource) =>
          `${resource.path}\0${resource.size}\0${resource.sha256}\n`,
      )
      .join(""),
  );
}

function parseCanonicalJson(filePath, label) {
  assertRegularFile(filePath, label);
  const source = fs.readFileSync(filePath, "utf8");
  let value;
  try {
    value = JSON.parse(source);
  } catch (error) {
    throw new Error(`${label} is not valid JSON: ${error.message}`);
  }
  if (source !== `${JSON.stringify(value, null, 2)}\n`) {
    throw new Error(`${label} is not canonical pretty-printed JSON`);
  }
  return { source, value };
}

function readArtifactLock(lockPath) {
  const { value: lock } = parseCanonicalJson(
    lockPath,
    "Lotus Next artifact lock",
  );
  assertExactKeys(lock, LOCK_KEYS, "Lotus Next artifact lock");
  if (lock.schemaVersion !== ARTIFACT_MANIFEST_SCHEMA_VERSION) {
    throw new Error("unsupported Lotus Next artifact lock schema");
  }
  if (lock.packageName !== LOTUS_NEXT_PACKAGE_NAME) {
    throw new Error(
      `artifact lock package must be exactly ${LOTUS_NEXT_PACKAGE_NAME}`,
    );
  }
  assertPackageVersion(lock.packageVersion);
  assertSourceRevision(lock.sourceRevision);
  if (lock.sourceDirty !== false) {
    throw new Error(
      "the committed Lotus Next artifact lock must require sourceDirty=false",
    );
  }
  if (lock.entrypoint !== "index.html") {
    throw new Error(
      "the committed Lotus Next artifact entrypoint must be index.html",
    );
  }
  assertSha256(lock.resourcesSha256, "artifact lock resourcesSha256");
  assertSha256(lock.manifestSha256, "artifact lock manifestSha256");
  return lock;
}

function verifyLotusNextArtifact({
  distDirectory,
  expectedIdentity = {},
  ignoredPaths = [],
}) {
  const manifestPath = path.join(distDirectory, ARTIFACT_MANIFEST_FILE);
  const { source, value: manifest } = parseCanonicalJson(
    manifestPath,
    "Lotus Next universal manifest",
  );
  assertExactKeys(manifest, MANIFEST_KEYS, "Lotus Next universal manifest");

  if (manifest.schemaVersion !== ARTIFACT_MANIFEST_SCHEMA_VERSION) {
    throw new Error(
      `unsupported Lotus Next manifest schema ${manifest.schemaVersion}`,
    );
  }
  if (manifest.packageName !== LOTUS_NEXT_PACKAGE_NAME) {
    throw new Error(
      `Lotus Next package name must be exactly ${LOTUS_NEXT_PACKAGE_NAME}`,
    );
  }
  assertPackageVersion(manifest.packageVersion);
  assertSourceRevision(manifest.sourceRevision);
  if (typeof manifest.sourceDirty !== "boolean") {
    throw new Error("Lotus Next sourceDirty must be a boolean");
  }
  if (manifest.entrypoint !== "index.html") {
    throw new Error("Lotus Next entrypoint must be exactly index.html");
  }
  assertSha256(manifest.resourcesSha256, "Lotus Next resourcesSha256");
  if (!Array.isArray(manifest.resources) || manifest.resources.length === 0) {
    throw new Error("Lotus Next resources must be a non-empty array");
  }

  let previousPath;
  for (const [index, resource] of manifest.resources.entries()) {
    assertExactKeys(resource, RESOURCE_KEYS, `Lotus Next resource ${index}`);
    assertResourcePath(resource.path);
    if (previousPath !== undefined && resource.path <= previousPath) {
      throw new Error(
        "Lotus Next resource paths must be unique and strictly sorted",
      );
    }
    if (!Number.isSafeInteger(resource.size) || resource.size < 0) {
      throw new Error(
        `Lotus Next resource ${resource.path} has an invalid size`,
      );
    }
    assertSha256(
      resource.sha256,
      `Lotus Next resource ${resource.path} digest`,
    );
    previousPath = resource.path;
  }
  if (
    !manifest.resources.some(
      (resource) => resource.path === manifest.entrypoint,
    )
  ) {
    throw new Error("Lotus Next resources do not contain index.html");
  }
  if (
    calculateResourcesSha256(manifest.resources) !== manifest.resourcesSha256
  ) {
    throw new Error(
      "Lotus Next combined resource digest does not match its records",
    );
  }

  const actualPaths = listResourcePaths(distDirectory, ignoredPaths);
  const declaredPaths = manifest.resources.map((resource) => resource.path);
  if (
    actualPaths.length !== declaredPaths.length ||
    actualPaths.some(
      (resourcePath, index) => resourcePath !== declaredPaths[index],
    )
  ) {
    throw new Error(
      "Lotus Next resource inventory does not match the universal manifest",
    );
  }
  for (const [index, resourcePath] of actualPaths.entries()) {
    const actual = resourceRecord(distDirectory, resourcePath);
    const declared = manifest.resources[index];
    if (actual.size !== declared.size || actual.sha256 !== declared.sha256) {
      throw new Error(
        `Lotus Next resource ${resourcePath} does not match its manifest`,
      );
    }
  }

  const manifestSha256 = sha256(source);
  const expectedValues = [
    ["packageName", manifest.packageName, expectedIdentity.packageName],
    [
      "packageVersion",
      manifest.packageVersion,
      expectedIdentity.packageVersion,
    ],
    [
      "sourceRevision",
      manifest.sourceRevision,
      expectedIdentity.sourceRevision,
    ],
    ["sourceDirty", manifest.sourceDirty, expectedIdentity.sourceDirty],
    ["entrypoint", manifest.entrypoint, expectedIdentity.entrypoint],
    [
      "resourcesSha256",
      manifest.resourcesSha256,
      expectedIdentity.resourcesSha256,
    ],
    ["manifestSha256", manifestSha256, expectedIdentity.manifestSha256],
  ];
  for (const [label, actual, expected] of expectedValues) {
    if (expected !== undefined && actual !== expected) {
      throw new Error(
        `Lotus Next ${label} ${JSON.stringify(actual)} does not match expected ${JSON.stringify(expected)}`,
      );
    }
  }

  return { manifest, manifestSha256 };
}

module.exports = {
  ARTIFACT_MANIFEST_FILE,
  LOTUS_NEXT_PACKAGE_NAME,
  calculateResourcesSha256,
  readArtifactLock,
  verifyLotusNextArtifact,
};
