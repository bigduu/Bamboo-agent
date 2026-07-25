#![cfg(feature = "test-utils")]

use std::collections::HashMap;

use bamboo_infrastructure::CommandEnvironmentDiagnostics;

type ClearCache = fn();
type PrimeCache = fn(HashMap<String, String>, CommandEnvironmentDiagnostics);

#[test]
fn legacy_command_environment_helpers_remain_available_at_all_public_paths() {
    let root_clear: ClearCache = bamboo_infrastructure::clear_command_environment_cache_for_tests;
    let process_clear: ClearCache =
        bamboo_infrastructure::process::clear_command_environment_cache_for_tests;
    let compatibility_clear: ClearCache =
        bamboo_infrastructure::process_utils::clear_command_environment_cache_for_tests;

    let root_prime: PrimeCache = bamboo_infrastructure::prime_command_environment_cache_for_tests;
    let process_prime: PrimeCache =
        bamboo_infrastructure::process::prime_command_environment_cache_for_tests;
    let compatibility_prime: PrimeCache =
        bamboo_infrastructure::process_utils::prime_command_environment_cache_for_tests;

    let _ = (
        root_clear,
        process_clear,
        compatibility_clear,
        root_prime,
        process_prime,
        compatibility_prime,
    );
}
