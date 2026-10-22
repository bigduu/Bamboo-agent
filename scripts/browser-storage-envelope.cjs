'use strict';

// Preparation for a platform storage adapter, not a provisioner or a launch
// authorization. Planning/validation are pure; inspection is read-only.
const assert = require('node:assert/strict');
const { createHash } = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path').posix;
const { isDeepStrictEqual } = require('node:util');
const { execFileSync } = require('node:child_process');

const IMAGE_BYTES = 536_870_912;
const QUOTA_BYTES = 402_653_184;
const ACCOUNT_KEY = createHash('sha256')
  .update('bamboo-browser-shared-account-v1\0').digest('hex');

function absolute(value) {
  assert.ok(typeof value === 'string' && value.length > 1 && !value.includes('\0') &&
    path.isAbsolute(value) && path.normalize(value) === value && !value.endsWith('/'),
  'Expected a canonical absolute path');
  return value;
}

function macSharedStoragePlan(dataRoot) {
  absolute(dataRoot);
  const root = path.join(dataRoot, 'browser-storage-v1');
  const image = path.join(root, 'account.dmg');
  const mount = path.join(root, 'mounted');
  const volumeName = `BambooBrowser_${createHash('sha256').update(dataRoot).digest('hex').slice(0, 12)}`;
  const paths = {
    profile: path.join(mount, 'profiles', ACCOUNT_KEY),
    home: path.join(mount, 'home'),
    cache: path.join(mount, 'cache'),
    config: path.join(mount, 'config'),
    data: path.join(mount, 'data'),
    temporary: path.join(mount, 'tmp'),
    downloads: path.join(mount, 'downloads'),
  };
  return {
    kind: 'mac_shared_browser_storage_plan_v1', dataRoot, root, image, mount,
    imageBytes: IMAGE_BYTES, quotaBytes: QUOTA_BYTES, accountKey: ACCOUNT_KEY, paths,
    // Preview only. Creation/mounting requires a separate concrete grant and
    // an adapter that checks exclusive ownership and actual OS readback.
    proposedCommands: [
      { executable: '/usr/bin/hdiutil', arguments: ['create', '-size', '512m',
        '-type', 'UDIF', '-fs', 'APFS', '-fsargs', `-q ${QUOTA_BYTES}`,
        '-volname', volumeName, image] },
      { executable: '/usr/bin/hdiutil', arguments: ['attach', '-plist', '-nobrowse',
        '-mountpoint', mount, image] },
    ],
    launchPaths: {
      userDataDir: paths.profile, downloadsPath: paths.downloads,
      environment: { HOME: paths.home, XDG_CACHE_HOME: paths.cache,
        XDG_CONFIG_HOME: paths.config, XDG_DATA_HOME: paths.data,
        TMPDIR: paths.temporary, TEMP: paths.temporary, TMP: paths.temporary,
        BAMBOO_BROWSER_PROFILE_DIR: paths.profile,
        BAMBOO_BROWSER_DOWNLOAD_ROOT: paths.downloads },
    },
  };
}

function keys(value, expected) {
  assert.ok(value && typeof value === 'object' && !Array.isArray(value));
  assert.deepEqual(Object.keys(value).sort(), [...expected].sort());
}

function privateDirectory(value, expectedPath, device, owner) {
  keys(value, ['path', 'realPath', 'directory', 'symlink', 'device', 'uid', 'mode']);
  assert.equal(value.path, expectedPath);
  assert.equal(value.realPath, expectedPath);
  assert.equal(value.directory, true);
  assert.equal(value.symlink, false);
  assert.equal(value.device, device);
  assert.equal(value.uid, owner);
  assert.ok(Number.isSafeInteger(value.mode) && value.mode >= 0 && value.mode <= 0xffff &&
    (value.mode & 0o777) === 0o700, 'Storage directories must be private');
}

// An OS adapter must collect these observations itself, with its stable lease
// held, binding hdiutil backing identity to diskutil volume/quota and opened
// path descriptors. Caller JSON, an old receipt or a mount path alone is not
// evidence. Passing this structural check never authorizes launch or detach.
function validateMacStorageObservation(plan, value) {
  assert.ok(isDeepStrictEqual(plan, macSharedStoragePlan(plan.dataRoot)), 'Storage plan changed');
  keys(value, ['ownerUid', 'image', 'attachment', 'volume', 'mount', 'paths']);
  assert.ok(Number.isSafeInteger(value.ownerUid) && value.ownerUid >= 0);
  const image = value.image;
  keys(image, ['path', 'realPath', 'regular', 'symlink', 'uid', 'mode', 'device',
    'sizeBytes', 'virtualBytes', 'format', 'segments']);
  assert.equal(image.path, plan.image);
  assert.equal(image.realPath, plan.image);
  assert.equal(image.regular, true);
  assert.equal(image.symlink, false);
  assert.equal(image.uid, value.ownerUid);
  assert.ok(Number.isSafeInteger(image.mode) && image.mode >= 0 && image.mode <= 0xffff);
  assert.equal(image.mode & 0o777, 0o600);
  assert.equal(image.sizeBytes, IMAGE_BYTES);
  assert.equal(image.virtualBytes, IMAGE_BYTES);
  assert.equal(image.format, 'UDRW');
  assert.deepEqual(image.segments, [plan.image]);
  const volume = value.volume;
  keys(volume, ['uuid', 'deviceIdentifier', 'quotaBytes', 'usedBytes', 'filesystem', 'writable']);
  assert.match(volume.uuid, /^[0-9A-F]{8}(?:-[0-9A-F]{4}){3}-[0-9A-F]{12}$/i);
  assert.match(volume.deviceIdentifier, /^disk[0-9]+s[0-9]+$/);
  assert.equal(volume.quotaBytes, QUOTA_BYTES);
  assert.ok(Number.isSafeInteger(volume.usedBytes) && volume.usedBytes >= 0 &&
    volume.usedBytes < QUOTA_BYTES, 'Full or unconfirmed storage refuses admission');
  assert.equal(volume.filesystem, 'apfs');
  assert.equal(volume.writable, true);
  keys(value.attachment, ['imagePath', 'volumeDevice', 'volumeUuid', 'mountPoint']);
  assert.equal(value.attachment.imagePath, plan.image);
  assert.equal(value.attachment.volumeDevice, volume.deviceIdentifier);
  assert.equal(value.attachment.volumeUuid, volume.uuid);
  assert.equal(value.attachment.mountPoint, plan.mount);
  const mount = value.mount;
  assert.ok(Number.isSafeInteger(mount.device) && mount.device > 0);
  assert.ok(Number.isSafeInteger(image.device) && image.device > 0);
  assert.notEqual(mount.device, image.device, 'An ordinary folder is not a mounted image');
  privateDirectory(mount, plan.mount, mount.device, value.ownerUid);
  keys(value.paths, Object.keys(plan.paths));
  for (const [role, target] of Object.entries(plan.paths)) {
    privateDirectory(value.paths[role], target, mount.device, value.ownerUid);
  }
  return { kind: 'validated_mac_storage_observation_v1', image: plan.image,
    volumeUuid: volume.uuid, imageBytes: IMAGE_BYTES, quotaBytes: QUOTA_BYTES,
    launchAuthorized: false, writerExitProven: false, detachAuthorized: false };
}

// These are the only OS subprocesses used by inspection. Creation, attachment,
// detachment, quota mutation and privilege escalation are intentionally absent.
function readMacPlist(executable, arguments_) {
  const environment = { PATH: '/usr/bin:/bin:/usr/sbin:/sbin', LC_ALL: 'C' };
  const bytes = execFileSync(executable, arguments_, { timeout: 10_000,
    maxBuffer: 4 * 1024 * 1024, stdio: ['ignore', 'pipe', 'pipe'], env: environment });
  const json = execFileSync('/usr/bin/plutil', ['-convert', 'json', '-o', '-', '-'], {
    input: bytes, encoding: 'utf8', timeout: 10_000, maxBuffer: 4 * 1024 * 1024,
    stdio: ['pipe', 'pipe', 'pipe'], env: environment,
  });
  return JSON.parse(json);
}

function inspectMacSharedStorage(plan, readers = {}) {
  assert.ok(isDeepStrictEqual(plan, macSharedStoragePlan(plan.dataRoot)), 'Storage plan changed');
  const filesystem = readers.filesystem || fs;
  const read = readers.readPlist || readMacPlist;
  const uid = readers.uid ?? process.getuid?.();
  assert.ok(Number.isSafeInteger(uid) && uid >= 0, 'Native owner identity unavailable');
  assert.equal(filesystem.realpathSync(plan.dataRoot), plan.dataRoot);
  const stat = filesystem.lstatSync(plan.image);
  const image = { path: plan.image, realPath: filesystem.realpathSync(plan.image),
    regular: stat.isFile(), symlink: stat.isSymbolicLink(), uid: stat.uid,
    mode: stat.mode, device: stat.dev, sizeBytes: stat.size };
  // Fail before subprocesses when the known owned backing is absent or unsafe.
  assert.equal(image.realPath, plan.image);
  assert.equal(image.regular, true);
  assert.equal(image.symlink, false);
  assert.equal(image.uid, uid);
  assert.equal(image.sizeBytes, IMAGE_BYTES);
  function directory(target) {
    const metadata = filesystem.lstatSync(target);
    return { path: target, realPath: filesystem.realpathSync(target),
      directory: metadata.isDirectory(), symlink: metadata.isSymbolicLink(),
      device: metadata.dev, uid: metadata.uid, mode: metadata.mode };
  }
  const mount = directory(plan.mount);
  const paths = Object.fromEntries(Object.entries(plan.paths)
    .map(([role, target]) => [role, directory(target)]));
  const imageInfo = read('/usr/bin/hdiutil', ['imageinfo', '-plist', plan.image]);
  image.virtualBytes = imageInfo['Size Information']?.['Total Bytes'];
  image.format = imageInfo.Format;
  image.segments = imageInfo.Segments;
  const info = read('/usr/sbin/diskutil', ['info', '-plist', plan.mount]);
  assert.equal(info.MountPoint, plan.mount);
  assert.equal(info.APFSSnapshot, false);
  const apfs = read('/usr/sbin/diskutil', ['apfs', 'list', '-plist']);
  const containers = apfs.Containers?.filter(item =>
    item.ContainerReference === info.APFSContainerReference);
  assert.equal(containers?.length, 1, 'Ambiguous or missing APFS container');
  const volumes = containers[0].Volumes?.filter(item => item.DeviceIdentifier === info.DeviceIdentifier);
  assert.equal(volumes?.length, 1, 'Ambiguous or missing APFS volume');
  const nativeVolume = volumes[0];
  assert.equal(nativeVolume.APFSVolumeUUID, info.VolumeUUID);
  const images = read('/usr/bin/hdiutil', ['info', '-plist']).images
    ?.filter(item => item['image-path'] === plan.image);
  assert.equal(images?.length, 1, 'Backing image not attached exactly once');
  const entities = images[0]['system-entities']?.filter(item =>
    item['dev-entry'] === `/dev/${info.DeviceIdentifier}` && item['mount-point'] === plan.mount);
  assert.equal(entities?.length, 1, 'Mount is not owned by the expected backing image');
  const observation = { ownerUid: uid, image, mount, paths,
    volume: { uuid: nativeVolume.APFSVolumeUUID, deviceIdentifier: nativeVolume.DeviceIdentifier,
      quotaBytes: nativeVolume.CapacityQuota, usedBytes: nativeVolume.CapacityInUse,
      filesystem: info.FilesystemType, writable: info.WritableVolume },
    attachment: { imagePath: images[0]['image-path'], volumeDevice: info.DeviceIdentifier,
      volumeUuid: info.VolumeUUID, mountPoint: info.MountPoint },
  };
  return { observation, validation: validateMacStorageObservation(plan, observation) };
}

module.exports = { IMAGE_BYTES, QUOTA_BYTES, ACCOUNT_KEY, macSharedStoragePlan,
  validateMacStorageObservation, inspectMacSharedStorage };
