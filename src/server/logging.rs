/// Initialize the logging system
pub fn init_logging(debug: bool) {
    let filter = if debug { "debug" } else { "info" };

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(filter))
        .format(|buf, record| {
            use std::io::Write;
            writeln!(
                buf,
                "[{}] {} [{}] {} - {}",
                chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f"),
                record.level(),
                record.target(),
                record.module_path().unwrap_or("unknown"),
                record.args()
            )
        })
        .init();
}
