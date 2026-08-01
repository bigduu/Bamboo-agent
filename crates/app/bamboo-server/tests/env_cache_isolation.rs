use std::sync::{mpsc, Arc, Barrier};
use std::time::Duration;

use bamboo_config::{Config, EnvVarEntry};

const CACHE_OWNER: &str = "BAMBOO_ENV_CACHE_TEST_OWNER";

struct ResetGlobalEnvCache;

impl Drop for ResetGlobalEnvCache {
    fn drop(&mut self) {
        Config::default().publish_env_vars();
    }
}

fn config_with_marker(value: &str, description: &str) -> Config {
    let mut config = Config::default();
    config.env_vars.push(EnvVarEntry {
        name: CACHE_OWNER.to_string(),
        value: value.to_string(),
        secret: false,
        value_encrypted: None,
        credential_ref: None,
        configured: true,
        description: Some(description.to_string()),
    });
    config
}

fn current_marker() -> (Option<String>, Option<String>) {
    let value = Config::current_env_vars().get(CACHE_OWNER).cloned();
    let description = Config::current_prompt_safe_env_vars()
        .into_iter()
        .find(|entry| entry.name == CACHE_OWNER)
        .and_then(|entry| entry.description);
    (value, description)
}

#[test]
fn scoped_env_cache_survives_parallel_global_publication_and_restores_nested_state() {
    // This dedicated integration-test binary intentionally exercises one
    // unscoped process-global publisher. Reset on entry and through RAII on
    // every exit. If more tests are ever added to this binary and run in
    // parallel, they must share a serialization guard around the global phase.
    Config::default().publish_env_vars();
    let reset_global_cache = ResetGlobalEnvCache;

    let global = config_with_marker("global-base", "global base metadata");
    global.publish_env_vars();
    assert_eq!(
        current_marker(),
        (
            Some("global-base".to_string()),
            Some("global base metadata".to_string())
        )
    );

    {
        let _outer = bamboo_config::test_support::isolate_env_vars_cache();
        config_with_marker("outer", "outer metadata").publish_env_vars();
        assert_eq!(
            current_marker(),
            (
                Some("outer".to_string()),
                Some("outer metadata".to_string())
            )
        );

        {
            let _inner = bamboo_config::test_support::isolate_env_vars_cache();
            config_with_marker("inner", "inner metadata").publish_env_vars();
            assert_eq!(
                current_marker(),
                (
                    Some("inner".to_string()),
                    Some("inner metadata".to_string())
                )
            );
        }

        assert_eq!(
            current_marker(),
            (
                Some("outer".to_string()),
                Some("outer metadata".to_string())
            ),
            "dropping an inner isolation scope must restore the outer snapshot"
        );
    }

    assert_eq!(
        current_marker(),
        (
            Some("global-base".to_string()),
            Some("global base metadata".to_string())
        ),
        "dropping the outer isolation scope must reveal the global snapshot"
    );

    const WORKERS: usize = 8;
    const ITERATIONS: usize = 128;
    let (result_tx, result_rx) = mpsc::channel();

    std::thread::spawn(move || {
        let phase = Arc::new(Barrier::new(WORKERS + 1));
        let workers = (0..WORKERS)
            .map(|worker| {
                let phase = Arc::clone(&phase);
                std::thread::spawn(move || {
                    let _isolation = bamboo_config::test_support::isolate_env_vars_cache();
                    let marker = format!("scoped-worker-{worker}");
                    let description = format!("scoped metadata {worker}");
                    let config = config_with_marker(&marker, &description);
                    let mut contaminated = false;

                    for _ in 0..ITERATIONS {
                        config.publish_env_vars();
                        phase.wait();
                        phase.wait();
                        contaminated |=
                            current_marker() != (Some(marker.clone()), Some(description.clone()));
                        phase.wait();
                    }

                    contaminated
                })
            })
            .collect::<Vec<_>>();

        let global_phase = Arc::clone(&phase);
        let global_publisher = std::thread::spawn(move || {
            let mut last = (String::new(), String::new());
            for iteration in 0..ITERATIONS {
                global_phase.wait();
                last = (
                    format!("global-publisher-{iteration}"),
                    format!("global metadata {iteration}"),
                );
                config_with_marker(&last.0, &last.1).publish_env_vars();
                global_phase.wait();
                global_phase.wait();
            }
            last
        });

        let worker_results = workers
            .into_iter()
            .map(std::thread::JoinHandle::join)
            .collect::<Vec<_>>();
        let global_result = global_publisher.join();
        let _ = result_tx.send((worker_results, global_result));
    });

    let (worker_results, global_result) = result_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("parallel env-cache rendezvous timed out");

    for result in worker_results {
        assert!(
            !result.expect("parallel scoped env-cache worker"),
            "a scoped env-cache worker observed another publisher's value or metadata"
        );
    }

    let global_result = global_result.expect("unscoped global env-cache publisher");
    assert_eq!(
        current_marker(),
        (Some(global_result.0), Some(global_result.1)),
        "the unscoped publisher must continue to update the process-global cache"
    );

    drop(reset_global_cache);
    assert_eq!(
        current_marker(),
        (None, None),
        "the standalone regression must not leave its global marker behind"
    );
}
