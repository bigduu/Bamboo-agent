'use strict';
// Inert readback fixtures only. No disk image, mount, browser or native writer.
const assert = require('node:assert/strict');
const { test } = require('node:test');
const { IMAGE_BYTES, QUOTA_BYTES, macSharedStoragePlan, validateMacStorageObservation,
  validateMacStorageResetObservation, validateMacStoragePreparationObservation,
  inspectMacStorageForPreparation, inspectMacStorageBacking } = require('./host.cjs');

function fixture() {
  const plan = macSharedStoragePlan('/synthetic/bamboo');
  const directory = p => ({ path: p, realPath: p, directory: true, symlink: false,
    device: 22, uid: 501, mode: 0o40700 });
  const uuid = '11111111-2222-4333-8444-555555555555';
  const observation = { ownerUid: 501,
    image: { path: plan.image, realPath: plan.image, regular: true, symlink: false, uid: 501,
      mode: 0o100600, device: 11, sizeBytes: IMAGE_BYTES, virtualBytes: IMAGE_BYTES,
      format: 'UDRW', segments: [plan.image] },
    attachment: { imagePath: plan.image, volumeDevice: 'disk11s1', volumeUuid: uuid, mountPoint: plan.mount },
    volume: { uuid, deviceIdentifier: 'disk11s1', quotaBytes: QUOTA_BYTES,
      usedBytes: 20480, filesystem: 'apfs', writable: true },
    mount: { ...directory(plan.mount), mode: 0o40755 },
    paths: Object.fromEntries(Object.keys(plan.paths).map(role => [role, null])),
  };
  return { plan, observation, directory };
}

test('fresh preparation inspects the exact empty bounded volume without admitting a writer', () => {
  const f = fixture(); const result = validateMacStoragePreparationObservation(f.plan, f.observation);
  assert.equal(result.kind, 'validated_mac_storage_preparation_observation_v1');
  assert.equal(result.launchAuthorized, false); assert.equal(result.writerExitProven, false);
  assert.equal(result.detachAuthorized, false);
  assert.throws(() => validateMacStorageObservation(f.plan, f.observation));
  assert.throws(() => validateMacStorageResetObservation(f.plan, f.observation));
});

test('preparation refuses writable/public or wrong-owner mount roots before native initialization', () => {
  for (const mutate of [x => { x.mount.mode = 0o40777; }, x => { x.mount.uid = 0; },
    x => { x.mount.symlink = true; }, x => { x.mount.realPath = '/synthetic/other'; }]) {
    const f = fixture(); mutate(f.observation);
    assert.throws(() => validateMacStoragePreparationObservation(f.plan, f.observation));
  }
});

test('preparation preserves exact backing, quota and attachment checks while roles are absent', () => {
  for (const mutate of [x => { x.image.format = 'SPRS'; }, x => { x.volume.quotaBytes++; },
    x => { x.attachment.imagePath += '.other'; }, x => { x.image.sizeBytes++; },
    x => { x.attachment.volumeUuid = 'AAAAAAAA-BBBB-4CCC-8DDD-EEEEEEEEEEEE'; }]) {
    const f = fixture(); mutate(f.observation);
    assert.throws(() => validateMacStoragePreparationObservation(f.plan, f.observation));
  }
});

test('existing role directories must be private and on the verified volume during preparation', () => {
  const f = fixture(); f.observation.paths.home = f.directory(f.plan.paths.home);
  assert.doesNotThrow(() => validateMacStoragePreparationObservation(f.plan, f.observation));
  for (const mutate of [x => { x.mode = 0o40755; }, x => { x.device = 11; }, x => { x.symlink = true; }]) {
    f.observation.paths.home = f.directory(f.plan.paths.home); mutate(f.observation.paths.home);
    assert.throws(() => validateMacStoragePreparationObservation(f.plan, f.observation));
  }
});

test('a full reattached volume can be identified without making it eligible for launch', () => {
  const f = fixture(); f.observation.volume.usedBytes = QUOTA_BYTES;
  assert.doesNotThrow(() => validateMacStoragePreparationObservation(f.plan, f.observation));
  f.observation.mount.mode = 0o40700;
  f.observation.paths = Object.fromEntries(Object.entries(f.plan.paths).map(([k, p]) => [k, f.directory(p)]));
  assert.throws(() => validateMacStorageObservation(f.plan, f.observation));
});

test('missing role metadata permits only ENOENT, never permissions or native read failures', () => {
  const f = fixture(); let queries = 0;
  const readers = { uid: 501, filesystem: { realpathSync: p => p,
    lstatSync(p) {
      if (p === f.plan.image) return { isFile: () => true, isSymbolicLink: () => false,
        uid: 501, mode: 0o100600, dev: 11, size: IMAGE_BYTES };
      if (p === f.plan.mount) return { isDirectory: () => true, isSymbolicLink: () => false,
        uid: 501, mode: 0o40755, dev: 22 };
      throw Object.assign(new Error('synthetic denied'), { code: 'EACCES' });
    } }, readPlist() { queries++; assert.fail('must fail before native queries'); } };
  assert.throws(() => inspectMacStorageForPreparation(f.plan, readers), e => e.code === 'EACCES');
  assert.equal(queries, 0);
});

function nativeFixture() {
  const f = fixture(); const calls = [];
  const images = [];
  const replies = { image: { Format: 'UDRW', Segments: [f.plan.image], 'Size Information': { 'Total Bytes': IMAGE_BYTES } },
    attachments: { images }, mount: { MountPoint: f.plan.mount, APFSSnapshot: false,
      APFSContainerReference: 'disk11', DeviceIdentifier: 'disk11s1', VolumeUUID: f.observation.volume.uuid,
      FilesystemType: 'apfs', WritableVolume: true },
    apfs: { Containers: [{ ContainerReference: 'disk11', Volumes: [{ DeviceIdentifier: 'disk11s1',
      APFSVolumeUUID: f.observation.volume.uuid, CapacityQuota: QUOTA_BYTES, CapacityInUse: 20480 }] }] } };
  const readers = { uid: 501, filesystem: { realpathSync: p => p,
    lstatSync(p) {
      if (p === f.plan.image) return { isFile: () => true, isSymbolicLink: () => false,
        uid: 501, mode: 0o100600, dev: 11, size: IMAGE_BYTES, ino: 71, nlink: 1 };
      if (p === f.plan.mount) return { isDirectory: () => true, isSymbolicLink: () => false,
        uid: 501, mode: 0o40755, dev: 22 };
      throw Object.assign(new Error('synthetic absent role'), { code: 'ENOENT' });
    } }, readPlist(executable, args) {
      calls.push([executable, ...args]);
      if (args[0] === 'imageinfo') return replies.image;
      if (executable === '/usr/bin/hdiutil') return replies.attachments;
      if (args[0] === 'apfs') return replies.apfs;
      return replies.mount;
    } };
  const attached = () => images.push({ 'image-path': f.plan.image,
    'system-entities': [{ 'dev-entry': '/dev/disk11s1', 'mount-point': f.plan.mount }] });
  return { ...f, readers, calls, replies, attached };
}

test('backing inspection distinguishes one unmounted registered file from its own exact attachment', () => {
  const f = nativeFixture();
  const first = inspectMacStorageBacking(f.plan, f.readers);
  assert.equal(first.attached, false); assert.equal(first.imageInode, 71);
  assert.equal(f.calls.length, 2);
  assert.ok(f.calls.every(([, action]) => ['imageinfo', 'info'].includes(action)));
  f.attached();
  assert.equal(inspectMacStorageBacking(f.plan, f.readers).attached, true);
});

test('backing inspection rejects another mount or duplicate attachments before native attach', () => {
  for (const mutate of [x => { x[0]['system-entities'][0]['mount-point'] = '/synthetic/other'; },
    x => { x.push(x[0]); }, x => { x[0]['system-entities'].push({ 'dev-entry': '/dev/disk12s1', 'mount-point': '/synthetic/extra' }); }]) {
    const f = nativeFixture(); f.attached(); mutate(f.replies.attachments.images);
    assert.throws(() => inspectMacStorageBacking(f.plan, f.readers));
  }
});

test('backing inspection rejects sparse/oversized/shared backing without trusting a ready journal', () => {
  const f = nativeFixture(); f.replies.image.Format = 'SPRS';
  assert.throws(() => inspectMacStorageBacking(f.plan, f.readers));
  f.replies.image.Format = 'UDRW'; f.replies.image['Size Information']['Total Bytes']++;
  assert.throws(() => inspectMacStorageBacking(f.plan, f.readers));
  const g = nativeFixture(); const stat = g.readers.filesystem.lstatSync;
  g.readers.filesystem.lstatSync = p => ({ ...stat(p), nlink: 2 });
  assert.throws(() => inspectMacStorageBacking(g.plan, g.readers));
  assert.equal(g.calls.length, 0);
});

test('fresh native inspection accepts only absent roles on its unique OS-bound volume', () => {
  const f = nativeFixture(); f.attached();
  const result = inspectMacStorageForPreparation(f.plan, f.readers);
  assert.equal(result.validation.kind, 'validated_mac_storage_preparation_observation_v1');
  assert.ok(Object.values(result.observation.paths).every(p => p === null));
  assert.equal(f.calls.length, 4);
  f.replies.apfs.Containers[0].Volumes.push({ DeviceIdentifier: 'disk11s2' });
  assert.throws(() => inspectMacStorageForPreparation(f.plan, f.readers));
});
