/// Initialize the logging system using tracing-subscriber.
pub fn init_logging(debug: bool) {
    use tracing_subscriber::EnvFilter;

    let filter = if debug { "debug" } else { "info" };

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(filter)),
        )
        .with_target(true)
        .init();
}
