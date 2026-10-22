'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { test } = require('node:test');
const { IMAGE_BYTES, QUOTA_BYTES, ACCOUNT_KEY, macSharedStoragePlan,
  validateMacStorageObservation, inspectMacSharedStorage } = require('./browser-storage-envelope.cjs');

// Inert normalized observations only: no image, mount, browser or native
// writer is created. These checks do not establish a real storage boundary.
function fixture() {
  const plan = macSharedStoragePlan('/synthetic/bamboo');
  const directory = target => ({ path: target, realPath: target, directory: true,
    symlink: false, device: 22, uid: 501, mode: 0o700 });
  const uuid = '11111111-2222-3333-4444-555555555555';
  return { plan, observation: { ownerUid: 501,
    image: { path: plan.image, realPath: plan.image, regular: true, symlink: false,
      uid: 501, mode: 0o600, device: 11, sizeBytes: IMAGE_BYTES,
      virtualBytes: IMAGE_BYTES, format: 'UDRW', segments: [plan.image] },
    attachment: { imagePath: plan.image, volumeDevice: 'disk11s1', volumeUuid: uuid,
      mountPoint: plan.mount },
    volume: { uuid, deviceIdentifier: 'disk11s1', quotaBytes: QUOTA_BYTES,
      usedBytes: 20_480, filesystem: 'apfs', writable: true },
    mount: directory(plan.mount),
    paths: Object.fromEntries(Object.entries(plan.paths).map(([role, target]) => [role, directory(target)])),
  } };
}

test('one account plan contains no chat/runtime/tmp-derived profile identity', () => {
  const a = macSharedStoragePlan('/synthetic/bamboo');
  assert.deepEqual(a, macSharedStoragePlan('/synthetic/bamboo'));
  assert.match(ACCOUNT_KEY, /^[a-f0-9]{64}$/);
  assert.equal(a.paths.profile, `${a.mount}/profiles/${ACCOUNT_KEY}`);
  assert.notEqual(a.image, macSharedStoragePlan('/synthetic/other-account').image);
  assert.deepEqual(a.proposedCommands[0].arguments.slice(0, 4), ['create', '-size', '512m', '-type']);
  assert.ok(a.proposedCommands[0].arguments.includes(`-q ${402653184}`));
  assert.equal(a.proposedCommands.length, 2);
});

test('every owned profile, HOME, cache, download and temporary path stays in the envelope', () => {
  const { plan } = fixture();
  for (const target of [plan.launchPaths.userDataDir, plan.launchPaths.downloadsPath,
    ...Object.values(plan.launchPaths.environment)]) assert.ok(target.startsWith(`${plan.mount}/`));
});

test('a structurally valid observation grants neither launch, writer exit nor detach', () => {
  const { plan, observation } = fixture();
  const result = validateMacStorageObservation(plan, observation);
  assert.equal(result.launchAuthorized, false);
  assert.equal(result.writerExitProven, false);
  assert.equal(result.detachAuthorized, false);
});

for (const [name, mutate] of [
  ['missing quota', x => { x.volume.quotaBytes = 0; }],
  ['larger quota', x => { x.volume.quotaBytes++; }],
  ['full volume', x => { x.volume.usedBytes = QUOTA_BYTES; }],
  ['sparse/other image type', x => { x.image.format = 'SPRS'; }],
  ['oversized backing', x => { x.image.sizeBytes++; }],
  ['unbounded virtual image', x => { x.image.virtualBytes++; }],
  ['additional backing segment', x => { x.image.segments.push('/synthetic/extra'); }],
  ['unrelated image attachment', x => { x.attachment.imagePath += '.other'; }],
  ['reused mount with different UUID', x => { x.attachment.volumeUuid = 'AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE'; }],
  ['different device', x => { x.attachment.volumeDevice = 'disk99s1'; }],
  ['ordinary unmounted directory', x => { x.mount.device = x.image.device; }],
  ['symlinked profile', x => { x.paths.profile.symlink = true; }],
  ['cache outside volume', x => { x.paths.cache.realPath = '/synthetic/outside'; }],
  ['cross-device temporary path', x => { x.paths.temporary.device++; }],
  ['wrong path owner', x => { x.paths.home.uid++; }],
  ['public profile directory', x => { x.paths.profile.mode = 0o755; }],
  ['coerced image mode', x => { x.image.mode = '384'; }],
  ['overflowing image mode', x => { x.image.mode += 2 ** 32; }],
  ['missing download identity', x => { delete x.paths.downloads; }],
  ['caller self-attestation', x => { x.ownerVerified = true; }],
]) test(`readback refuses ${name}`, () => {
  const { plan, observation } = fixture(); mutate(observation);
  assert.throws(() => validateMacStorageObservation(plan, observation));
});

test('caller cannot weaken the fixed plan or launch path mapping', () => {
  const { plan, observation } = fixture();
  plan.quotaBytes++;
  assert.throws(() => validateMacStorageObservation(plan, observation));
});

test('relative, aliased, root, and traversal inputs are rejected before producing commands', () => {
  for (const invalid of ['relative', '/', '/synthetic/../bamboo', '/synthetic/bamboo/', '/bad\0path']) {
    assert.throws(() => macSharedStoragePlan(invalid));
  }
});

function nativeFixture() {
  const { plan, observation } = fixture();
  const calls = [];
  const replies = {
    imageinfo: { Format: 'UDRW', Segments: [plan.image],
      'Size Information': { 'Total Bytes': IMAGE_BYTES } },
    mountinfo: { MountPoint: plan.mount, APFSSnapshot: false, VolumeUUID: observation.volume.uuid,
      APFSContainerReference: 'disk11', DeviceIdentifier: 'disk11s1',
      FilesystemType: 'apfs', WritableVolume: true },
    apfs: { Containers: [{ ContainerReference: 'disk11', Volumes: [{
      DeviceIdentifier: 'disk11s1', APFSVolumeUUID: observation.volume.uuid,
      CapacityQuota: QUOTA_BYTES, CapacityInUse: 20_480,
    }] }] },
    attachments: { images: [{ 'image-path': plan.image, 'system-entities': [
      { 'dev-entry': '/dev/disk11s1', 'mount-point': plan.mount },
    ] }] },
  };
  const readers = { uid: 501,
    filesystem: {
      realpathSync: value => value,
      lstatSync(value) {
        const image = value === plan.image;
        return { isFile: () => image, isDirectory: () => !image,
          isSymbolicLink: () => false, uid: 501, mode: image ? 0o100600 : 0o40700,
          dev: image ? 11 : 22, size: image ? IMAGE_BYTES : 0 };
      },
    },
    readPlist(executable, args) {
      calls.push([executable, ...args]);
      if (executable === '/usr/bin/hdiutil' && args[0] === 'imageinfo') return replies.imageinfo;
      if (executable === '/usr/bin/hdiutil' && args.join(' ') === 'info -plist') return replies.attachments;
      if (executable === '/usr/sbin/diskutil' && args[0] === 'info') return replies.mountinfo;
      if (executable === '/usr/sbin/diskutil' && args.join(' ') === 'apfs list -plist') return replies.apfs;
      assert.fail('Unexpected OS operation');
    },
  };
  return { plan, calls, readers, replies };
}

test('inspector binds actual OS field shapes through four read-only operations', () => {
  const f = nativeFixture();
  const { validation } = inspectMacSharedStorage(f.plan, f.readers);
  assert.equal(validation.launchAuthorized, false);
  assert.deepEqual(f.calls, [
    ['/usr/bin/hdiutil', 'imageinfo', '-plist', f.plan.image],
    ['/usr/sbin/diskutil', 'info', '-plist', f.plan.mount],
    ['/usr/sbin/diskutil', 'apfs', 'list', '-plist'],
    ['/usr/bin/hdiutil', 'info', '-plist'],
  ]);
});

for (const [name, mutate] of [
  ['duplicate container identity', x => { x.apfs.Containers.push(x.apfs.Containers[0]); }],
  ['different backing attachment', x => { x.attachments.images[0]['image-path'] += '.other'; }],
  ['snapshot instead of writable volume', x => { x.mountinfo.APFSSnapshot = true; }],
  ['read-only filesystem', x => { x.mountinfo.WritableVolume = false; }],
  ['missing kernel quota', x => { delete x.apfs.Containers[0].Volumes[0].CapacityQuota; }],
]) test(`native inspector refuses ${name}`, () => {
  const f = nativeFixture(); mutate(f.replies);
  assert.throws(() => inspectMacSharedStorage(f.plan, f.readers));
});

test('a metadata query failure never falls back to an ordinary directory', () => {
  const f = nativeFixture();
  f.readers.readPlist = () => { throw new Error('synthetic OS read failure'); };
  assert.throws(() => inspectMacSharedStorage(f.plan, f.readers), /synthetic OS read failure/);
});

test('absent backing fails before any OS query and does not provision files', t => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'bamboo-envelope-readonly-'));
  t.after(() => fs.rmdirSync(root));
  const plan = macSharedStoragePlan(fs.realpathSync(root));
  let queries = 0;
  assert.throws(() => inspectMacSharedStorage(plan, {
    uid: process.getuid?.() ?? 0,
    readPlist: () => { queries++; assert.fail('No OS query allowed without backing'); },
  }), error => error.code === 'ENOENT');
  assert.equal(queries, 0);
  assert.deepEqual(fs.readdirSync(root), []);
});
