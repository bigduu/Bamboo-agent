use super::WebService;

#[test]
fn test_web_service_lifecycle() {
    let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
    let ws = WebService::new(temp_dir.path().to_path_buf());
    assert!(!ws.is_running());
}
