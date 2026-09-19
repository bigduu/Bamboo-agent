#!/usr/bin/env node

const crypto = require("node:crypto");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { spawnSync } = require("node:child_process");

const {
  LOTUS_NEXT_PACKAGE_NAME,
  readArtifactLock,
  verifyLotusNextArtifact,
} = require("./lotus-next-artifact.cjs");

const ROOT = path.resolve(__dirname, "..");
// bamboo-server owns the package it embeds. Keeping the bytes inside the crate
// makes the source checkout, `cargo package`, and downstream installs use the
// same platform-safe path instead of relying on workspace-relative symlinks.
const OUTPUT_DIR = path.join(
  ROOT,
  "crates",
  "app",
  "bamboo-server",
  "frontend_package",
);
const OUTPUT_ZIP = path.join(OUTPUT_DIR, "lotus-frontend.zip");
const OUTPUT_MANIFEST = path.join(OUTPUT_DIR, "frontend-manifest.json");
const ARTIFACT_LOCK_PATH = path.join(__dirname, "frontend-package-lock.json");
const FRONTEND_MANIFEST_FILE = "frontend-manifest.json";
const LEGACY_LOTUS_PACKAGE_NAME = "@bigduu/lotus";

// The normal checkout/build path verifies and reuses the committed, immutable
// Lotus Next artifact. Developers and transitional release workflows must opt
// into another producer explicitly.
const SOURCE_MODE = (process.env.LOTUS_SOURCE || "committed").toLowerCase();
const LOCAL_PATH = path.resolve(
  ROOT,
  process.env.LOTUS_LOCAL_PATH || "../lotus-next",
);
const PACKAGE_NAME = process.env.LOTUS_PACKAGE_NAME || LOTUS_NEXT_PACKAGE_NAME;
const PREBUILT_DIST_DIR = process.env.LOTUS_DIST_DIR
  ? path.resolve(ROOT, process.env.LOTUS_DIST_DIR)
  : path.join(LOCAL_PATH, "dist");

function fail(message) {
  throw new Error(message);
}

function fileExists(target) {
  try {
    const metadata = fs.lstatSync(target);
    return metadata.isFile() && !metadata.isSymbolicLink();
  } catch {
    return false;
  }
}

function dirExists(target) {
  try {
    const metadata = fs.lstatSync(target);
    return metadata.isDirectory() && !metadata.isSymbolicLink();
  } catch {
    return false;
  }
}

function localLotusExists() {
  return (
    dirExists(LOCAL_PATH) && fileExists(path.join(LOCAL_PATH, "package.json"))
  );
}

function resolvePackageRoot() {
  try {
    const packageJsonPath = require.resolve(`${PACKAGE_NAME}/package.json`, {
      paths: [ROOT],
    });
    return path.dirname(packageJsonPath);
  } catch {
    return null;
  }
}

function readPackageJson(packageRoot, label) {
  const packageJsonPath = path.join(packageRoot, "package.json");
  if (!fileExists(packageJsonPath)) {
    fail(`${label} package.json is missing at ${packageJsonPath}`);
  }

  let packageJson;
  try {
    packageJson = JSON.parse(fs.readFileSync(packageJsonPath, "utf8"));
  } catch (error) {
    fail(`${label} package.json is invalid: ${error.message}`);
  }
  if (packageJson.name !== PACKAGE_NAME) {
    fail(
      `${label} package name ${JSON.stringify(packageJson.name)} does not match requested ${JSON.stringify(PACKAGE_NAME)}`,
    );
  }
  if (
    typeof packageJson.version !== "string" ||
    packageJson.version.length === 0
  ) {
    fail(`${label} package version is missing`);
  }
  return packageJson;
}

function runNpmScript(prefix, script) {
  const result = spawnSync("npm", ["run", script], {
    stdio: "inherit",
    env: process.env,
    cwd: prefix,
    shell: process.platform === "win32",
  });
  if (result.status !== 0) {
    fail(`Failed to run npm script ${JSON.stringify(script)} in ${prefix}`);
  }
}

function resolveSource() {
  if (!["committed", "auto", "local", "package"].includes(SOURCE_MODE)) {
    fail(
      `Invalid LOTUS_SOURCE=${JSON.stringify(SOURCE_MODE)} (expected committed|auto|local|package)`,
    );
  }
  if (SOURCE_MODE === "committed") return { mode: "committed" };

  const localAvailable = localLotusExists();
  const packageRoot = resolvePackageRoot();
  if (SOURCE_MODE === "local") {
    if (!localAvailable) {
      fail(
        `LOTUS_SOURCE=local but ${PACKAGE_NAME} was not found at ${LOCAL_PATH}. Set LOTUS_LOCAL_PATH or use LOTUS_SOURCE=package.`,
      );
    }
    return { mode: "local", packageRoot: LOCAL_PATH };
  }
  if (SOURCE_MODE === "package") {
    if (!packageRoot) {
      fail(
        `LOTUS_SOURCE=package but package ${JSON.stringify(PACKAGE_NAME)} is not installed. Install it or choose an explicit source.`,
      );
    }
    return { mode: "package", packageRoot };
  }
  if (localAvailable) return { mode: "local", packageRoot: LOCAL_PATH };
  if (packageRoot) return { mode: "package", packageRoot };
  return { mode: "committed" };
}

function comparePortablePath(left, right) {
  return Buffer.compare(Buffer.from(left, "utf8"), Buffer.from(right, "utf8"));
}

function listFilesRecursively(rootDir, ignoredPaths = []) {
  const ignored = new Set(ignoredPaths);
  const files = [];
  const walk = (currentDir) => {
    const entries = fs
      .readdirSync(currentDir, { withFileTypes: true })
      .sort((left, right) => comparePortablePath(left.name, right.name));
    for (const entry of entries) {
      const absolutePath = path.join(currentDir, entry.name);
      const relativePath = path
        .relative(rootDir, absolutePath)
        .replace(/\\/g, "/");
      if (ignored.has(relativePath)) continue;
      if (entry.isSymbolicLink()) {
        fail(`Frontend resource ${relativePath} must not be a symbolic link`);
      }
      if (entry.isDirectory()) {
        walk(absolutePath);
      } else if (entry.isFile()) {
        files.push({ absolutePath, relativePath });
      } else {
        fail(`Frontend resource ${relativePath} must be a regular file`);
      }
    }
  };

  walk(rootDir);
  return files.sort((left, right) =>
    comparePortablePath(left.relativePath, right.relativePath),
  );
}

function computeDirectoryHash(rootDir, ignoredPaths = []) {
  const hash = crypto.createHash("sha256");
  for (const file of listFilesRecursively(rootDir, ignoredPaths)) {
    hash.update(file.relativePath);
    hash.update("\0");
    hash.update(fs.readFileSync(file.absolutePath));
    hash.update("\0");
  }
  return `sha256:${hash.digest("hex")}`;
}

function frontendNameForPackage() {
  if (PACKAGE_NAME === LOTUS_NEXT_PACKAGE_NAME) return "lotus-next";
  if (PACKAGE_NAME === LEGACY_LOTUS_PACKAGE_NAME) return "lotus";
  fail(
    `Unsupported frontend package ${JSON.stringify(PACKAGE_NAME)}; expected ${LOTUS_NEXT_PACKAGE_NAME} or ${LEGACY_LOTUS_PACKAGE_NAME}`,
  );
}

function verifyDist(distDirectory, packageJson, requirePinnedIdentity) {
  if (!dirExists(distDirectory)) {
    fail(`Frontend dist directory not found: ${distDirectory}`);
  }
  if (!fileExists(path.join(distDirectory, "index.html"))) {
    fail(`Frontend dist is missing index.html: ${distDirectory}`);
  }

  if (PACKAGE_NAME !== LOTUS_NEXT_PACKAGE_NAME) return;

  const expectedIdentity = {
    packageName: packageJson.name,
    packageVersion: packageJson.version,
    sourceDirty: false,
  };
  if (requirePinnedIdentity) {
    Object.assign(expectedIdentity, readArtifactLock(ARTIFACT_LOCK_PATH));
  }
  verifyLotusNextArtifact({ distDirectory, expectedIdentity });
}

function writeFrontendManifest(stageDir, packageRoot, packageJson) {
  const manifest = {
    schema_version: 1,
    frontend_name: frontendNameForPackage(),
    frontend_version: packageJson.version,
    bundle_hash: computeDirectoryHash(stageDir),
    built_at: new Date().toISOString(),
    entry: "index.html",
  };
  const content = `${JSON.stringify(manifest, null, 2)}\n`;
  fs.writeFileSync(path.join(stageDir, FRONTEND_MANIFEST_FILE), content);
  fs.writeFileSync(path.join(packageRoot, FRONTEND_MANIFEST_FILE), content);
  return manifest;
}

function createZipFromStage(stageDir, zipPath) {
  const result =
    process.platform === "win32"
      ? spawnSync("7z", ["a", "-tzip", zipPath, "."], {
          cwd: stageDir,
          stdio: "inherit",
          env: process.env,
          shell: true,
        })
      : spawnSync("zip", ["-q", "-r", zipPath, "."], {
          cwd: stageDir,
          stdio: "inherit",
          env: process.env,
        });
  if (result.status !== 0) {
    fail(`Failed to create frontend zip package from ${stageDir}`);
  }
}

function validatePortableArchiveEntry(rawName) {
  const isDirectory = rawName.endsWith("/");
  const name = isDirectory ? rawName.slice(0, -1) : rawName;
  if (
    name.length === 0 ||
    name.startsWith("/") ||
    /^[A-Za-z]:/.test(name) ||
    name.includes("\\") ||
    /[\0-\x1f\x7f]/.test(name)
  ) {
    fail(
      `Frontend archive contains a non-portable path: ${JSON.stringify(rawName)}`,
    );
  }
  const segments = name.split("/");
  if (
    segments.some(
      (segment) =>
        segment.length === 0 ||
        segment === "." ||
        segment === ".." ||
        segment.endsWith(".") ||
        segment.endsWith(" ") ||
        /[<>:"|?*]/.test(segment),
    )
  ) {
    fail(
      `Frontend archive contains an unsafe path: ${JSON.stringify(rawName)}`,
    );
  }
  return { isDirectory, name };
}

function inspectZipPaths(zipPath) {
  if (process.platform === "win32") return;
  const result = spawnSync("unzip", ["-Z1", zipPath], {
    encoding: "utf8",
    env: process.env,
  });
  if (result.status !== 0) {
    fail(
      `Cannot inspect frontend zip archive ${zipPath}: ${result.stderr || result.stdout}`,
    );
  }

  const seen = new Map();
  for (const rawName of result.stdout.split(/\r?\n/).filter(Boolean)) {
    const { isDirectory, name } = validatePortableArchiveEntry(rawName);
    const key = name.toLowerCase();
    const previous = seen.get(key);
    if (
      previous &&
      (previous.name !== name || !isDirectory || !previous.isDirectory)
    ) {
      fail(
        `Frontend archive paths ${JSON.stringify(previous.name)} and ${JSON.stringify(name)} collide`,
      );
    }
    seen.set(key, { isDirectory, name });
  }
}

function extractZip(zipPath, targetDir) {
  fs.mkdirSync(targetDir, { recursive: true });
  const result =
    process.platform === "win32"
      ? spawnSync("7z", ["x", "-y", `-o${targetDir}`, zipPath], {
          stdio: "inherit",
          env: process.env,
          shell: true,
        })
      : spawnSync("unzip", ["-q", zipPath, "-d", targetDir], {
          stdio: "inherit",
          env: process.env,
        });
  if (result.status !== 0) {
    fail(`Failed to extract frontend zip package ${zipPath}`);
  }
}

function readFrontendManifest(manifestPath) {
  if (!fileExists(manifestPath)) {
    fail(`Frontend manifest not found: ${manifestPath}`);
  }
  const source = fs.readFileSync(manifestPath, "utf8");
  let manifest;
  try {
    manifest = JSON.parse(source);
  } catch (error) {
    fail(`Frontend manifest is invalid JSON: ${error.message}`);
  }
  if (source !== `${JSON.stringify(manifest, null, 2)}\n`) {
    fail("Frontend manifest must be canonical pretty-printed JSON");
  }
  const expectedKeys = [
    "schema_version",
    "frontend_name",
    "frontend_version",
    "bundle_hash",
    "built_at",
    "entry",
  ];
  if (
    Object.keys(manifest).length !== expectedKeys.length ||
    Object.keys(manifest).some((key, index) => key !== expectedKeys[index])
  ) {
    fail(`Frontend manifest must contain exactly: ${expectedKeys.join(", ")}`);
  }
  let canonicalBuiltAt = null;
  try {
    canonicalBuiltAt = new Date(manifest.built_at).toISOString();
  } catch {
    // The aggregate validation below reports a stable, field-level error.
  }
  if (
    manifest.schema_version !== 1 ||
    manifest.frontend_name !== frontendNameForPackage() ||
    typeof manifest.frontend_version !== "string" ||
    !/^sha256:[0-9a-f]{64}$/.test(manifest.bundle_hash) ||
    typeof manifest.built_at !== "string" ||
    canonicalBuiltAt !== manifest.built_at ||
    manifest.entry !== "index.html"
  ) {
    fail("Frontend manifest identity or integrity fields are invalid");
  }
  return { manifest, source };
}

function validateStagedPackage(packageRoot, expectedIdentity) {
  const zipPath = path.join(packageRoot, "lotus-frontend.zip");
  const sidecarPath = path.join(packageRoot, FRONTEND_MANIFEST_FILE);
  if (!fileExists(zipPath)) fail(`Frontend zip not found: ${zipPath}`);
  const { manifest, source: sidecarSource } = readFrontendManifest(sidecarPath);
  if (manifest.frontend_version !== expectedIdentity.packageVersion) {
    fail(
      `Embedded frontend version ${JSON.stringify(manifest.frontend_version)} does not match expected ${JSON.stringify(expectedIdentity.packageVersion)}`,
    );
  }

  inspectZipPaths(zipPath);
  const extractionRoot = fs.mkdtempSync(
    path.join(os.tmpdir(), "bamboo-frontend-verify-"),
  );
  try {
    extractZip(zipPath, extractionRoot);
    const embeddedManifestPath = path.join(
      extractionRoot,
      FRONTEND_MANIFEST_FILE,
    );
    if (!fileExists(embeddedManifestPath)) {
      fail(`Frontend zip is missing ${FRONTEND_MANIFEST_FILE}`);
    }
    if (fs.readFileSync(embeddedManifestPath, "utf8") !== sidecarSource) {
      fail(
        "Frontend sidecar manifest does not match the manifest inside the zip byte-for-byte",
      );
    }
    if (!fileExists(path.join(extractionRoot, manifest.entry))) {
      fail(`Frontend zip is missing manifest entry ${manifest.entry}`);
    }
    const actualHash = computeDirectoryHash(extractionRoot, [
      FRONTEND_MANIFEST_FILE,
    ]);
    if (actualHash !== manifest.bundle_hash) {
      fail(
        `Frontend bundle hash ${JSON.stringify(actualHash)} does not match ${JSON.stringify(manifest.bundle_hash)}`,
      );
    }

    if (PACKAGE_NAME === LOTUS_NEXT_PACKAGE_NAME) {
      verifyLotusNextArtifact({
        distDirectory: extractionRoot,
        expectedIdentity,
        ignoredPaths: [FRONTEND_MANIFEST_FILE],
      });
    }
  } finally {
    fs.rmSync(extractionRoot, { recursive: true, force: true });
  }
  return manifest;
}

function replaceCommittedPackage(validatedPackageRoot) {
  fs.mkdirSync(path.dirname(OUTPUT_DIR), { recursive: true });
  const backupDir = `${OUTPUT_DIR}.backup-${process.pid}-${Date.now()}`;
  const hadPreviousPackage = fs.existsSync(OUTPUT_DIR);
  if (hadPreviousPackage) fs.renameSync(OUTPUT_DIR, backupDir);
  try {
    fs.renameSync(validatedPackageRoot, OUTPUT_DIR);
  } catch (error) {
    if (hadPreviousPackage && !fs.existsSync(OUTPUT_DIR)) {
      fs.renameSync(backupDir, OUTPUT_DIR);
    }
    throw error;
  }
  if (hadPreviousPackage)
    fs.rmSync(backupDir, { recursive: true, force: true });
}

function committedExpectedIdentity() {
  return readArtifactLock(ARTIFACT_LOCK_PATH);
}

function verifyCommittedPackage(reason) {
  if (!fileExists(OUTPUT_ZIP) || !fileExists(OUTPUT_MANIFEST)) {
    fail(
      `${reason} The committed Lotus Next package is incomplete; stage the exact published package explicitly.`,
    );
  }
  const expectedIdentity = committedExpectedIdentity();
  const manifest = validateStagedPackage(OUTPUT_DIR, expectedIdentity);
  console.log(`ℹ️ ${reason}`);
  console.log(`✅ Verified committed Lotus Next package: ${OUTPUT_ZIP}`);
  console.log(
    `ℹ️ Embedded frontend: ${manifest.frontend_name}@${manifest.frontend_version} (${expectedIdentity.sourceRevision})`,
  );
}

function stagePackageFromDist(
  distDirectory,
  packageJson,
  requirePinnedIdentity,
) {
  const expectedIdentity =
    PACKAGE_NAME === LOTUS_NEXT_PACKAGE_NAME && requirePinnedIdentity
      ? committedExpectedIdentity()
      : {
          packageName: packageJson.name,
          packageVersion: packageJson.version,
          ...(PACKAGE_NAME === LOTUS_NEXT_PACKAGE_NAME
            ? { sourceDirty: false }
            : {}),
        };
  verifyDist(distDirectory, packageJson, requirePinnedIdentity);

  const workspace = fs.mkdtempSync(
    path.join(path.dirname(OUTPUT_DIR), ".frontend-package-stage-"),
  );
  const stageDir = path.join(workspace, "stage");
  const packageRoot = path.join(workspace, "package");
  fs.mkdirSync(stageDir, { recursive: true });
  fs.mkdirSync(packageRoot, { recursive: true });
  try {
    fs.cpSync(distDirectory, stageDir, { recursive: true });
    const manifest = writeFrontendManifest(stageDir, packageRoot, packageJson);
    createZipFromStage(stageDir, path.join(packageRoot, "lotus-frontend.zip"));
    validateStagedPackage(packageRoot, expectedIdentity);
    fs.rmSync(stageDir, { recursive: true, force: true });
    replaceCommittedPackage(packageRoot);

    console.log(`✅ Created embedded frontend package: ${OUTPUT_ZIP}`);
    console.log(`✅ Wrote embedded frontend manifest: ${OUTPUT_MANIFEST}`);
    console.log(
      `ℹ️ Embedded frontend: ${manifest.frontend_name}@${manifest.frontend_version}`,
    );
    console.log(`ℹ️ Embedded frontend hash: ${manifest.bundle_hash}`);
  } finally {
    fs.rmSync(workspace, { recursive: true, force: true });
  }
}

function stagePackage() {
  const source = resolveSource();
  if (source.mode === "committed") {
    verifyCommittedPackage(
      SOURCE_MODE === "auto"
        ? `No explicit local or installed ${PACKAGE_NAME} source was found.`
        : "LOTUS_SOURCE=committed is the default reproducible build path.",
    );
    return;
  }

  const packageJson = readPackageJson(
    source.packageRoot,
    source.mode === "local" ? "Local frontend" : "Installed frontend",
  );
  if (source.mode === "local") {
    console.log(`ℹ️ Building local ${PACKAGE_NAME} at ${source.packageRoot}`);
    runNpmScript(source.packageRoot, "build");
  } else {
    console.log(`ℹ️ Using installed ${PACKAGE_NAME} at ${source.packageRoot}`);
  }
  stagePackageFromDist(
    path.join(source.packageRoot, "dist"),
    packageJson,
    source.mode === "package" && PACKAGE_NAME === LOTUS_NEXT_PACKAGE_NAME,
  );
}

function stagePrebuiltPackage() {
  const packageJson = readPackageJson(LOCAL_PATH, "Prebuilt frontend");
  stagePackageFromDist(PREBUILT_DIST_DIR, packageJson, false);
}

function printInfo() {
  const packageRoot = resolvePackageRoot();
  console.log(`LOTUS_SOURCE=${SOURCE_MODE}`);
  console.log(`FRONTEND_PACKAGE_DIR=${OUTPUT_DIR}`);
  console.log(
    `LOTUS_LOCAL_PATH=${LOCAL_PATH} (${localLotusExists() ? "found" : "missing"})`,
  );
  console.log(
    `LOTUS_PACKAGE_NAME=${PACKAGE_NAME} (${packageRoot ? `found at ${packageRoot}` : "missing"})`,
  );
  console.log(`LOTUS_ARTIFACT_LOCK=${ARTIFACT_LOCK_PATH}`);
}

function main() {
  const command = process.argv[2] || "stage";
  if (command === "stage") return stagePackage();
  if (command === "stage:prebuilt") return stagePrebuiltPackage();
  if (command === "verify") {
    verifyCommittedPackage("Verifying the committed default frontend package.");
    return;
  }
  if (command === "info") return printInfo();
  fail(
    `Unknown command ${JSON.stringify(command)}. Use one of: stage, stage:prebuilt, verify, info.`,
  );
}

try {
  main();
} catch (error) {
  console.error(`❌ ${error.message}`);
  process.exit(1);
}
