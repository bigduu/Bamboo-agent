fn main() {
    if let Err(error) = bamboo_tool_event_recorder::run_from_env() {
        eprintln!("tool-event-recorder stopped: {error}");
        std::process::exit(1);
    }
}
