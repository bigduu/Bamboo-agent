'use strict';

// One canonical implementation is included in the three-file published host.
// Requiring it does not load Playwright or start any native process.
const { IMAGE_BYTES, QUOTA_BYTES, ACCOUNT_KEY, macSharedStoragePlan,
  validateMacStorageObservation, inspectMacSharedStorage,
  validateMacStorageResetObservation, inspectMacStorageForReset,
  validateMacStoragePreparationObservation, inspectMacStorageForPreparation,
  inspectMacStorageBacking } = require('../browser-runtime/host.cjs');

module.exports = { IMAGE_BYTES, QUOTA_BYTES, ACCOUNT_KEY, macSharedStoragePlan,
  validateMacStorageObservation, inspectMacSharedStorage,
  validateMacStorageResetObservation, inspectMacStorageForReset,
  validateMacStoragePreparationObservation, inspectMacStorageForPreparation,
  inspectMacStorageBacking };
