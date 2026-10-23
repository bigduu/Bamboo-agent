'use strict';
// A JavaScript action receives the scoped Actions artifact runtime credentials.
// They are deliberately neither exported to shell steps nor written to receipts.
const input = name => process.env['INPUT_' + name.toUpperCase()] || '';
const command = input('command'), directory = input('directory');
const args = command === 'build' ? [command, input('profile'), directory]
  : command === 'run' ? [command, input('plan-id'), input('plan-key'), input('block-id'), directory]
    : [command, input('plan-id'), input('plan-key'), directory];
require('../../../scripts/test-groups-ci.cjs').main(args).catch(e => { console.error(e.message); process.exitCode = 1; });
