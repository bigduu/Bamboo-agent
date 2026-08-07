use std::path::PathBuf;

use bamboo_infrastructure::logging::LogOptions;

#[test]
fn log_options_remains_constructible_with_the_legacy_public_fields() {
    let options = LogOptions {
        dir: PathBuf::from("logs"),
        file_name_prefix: "bamboo".to_string(),
        max_files: 14,
        default_level: "info".to_string(),
    };

    assert_eq!(options.dir, PathBuf::from("logs"));
}
