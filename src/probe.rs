use std::time::Instant;

use crate::config::{now_epoch, AppConfig, InstanceConfig, SharedState};
use crate::database;

/// Probe a single instance's /health endpoint. Returns true if healthy.
pub async fn probe_instance(
    state: &SharedState,
    instance: &InstanceConfig,
    client: &reqwest::Client,
    config: &AppConfig,
) -> bool {
    let url = format!("http://127.0.0.1:{}/health", instance.port);

    let start = Instant::now();
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(config.probe.timeout_seconds),
        client.get(&url).send(),
    )
    .await;

    match result {
        Ok(Ok(resp)) => {
            let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
            let status = resp.status().as_u16();
            let success = (200..300).contains(&status);

            let _ = database::insert_probe_result(
                &state.db,
                &instance.name,
                Some(status),
                Some(elapsed_ms),
                success,
                None,
            )
            .await;

            let mut states = state.instance_states.write().await;
            let ist = states.entry(instance.name.clone()).or_default();
            ist.last_check = Some(now_epoch());
            ist.last_response_time_ms = Some((elapsed_ms * 100.0).round() / 100.0);
            ist.last_status_code = Some(status);

            if success {
                if ist.online != Some(true) {
                    ist.uptime_since = Some(now_epoch());
                    if ist.online == Some(false) {
                        let _ = database::insert_event(
                            &state.db,
                            "recovery",
                            &format!("实例 {} 恢复在线", instance.name),
                            Some(&instance.name),
                            None,
                        )
                        .await;
                    }
                }
                ist.online = Some(true);
                ist.consecutive_failures = 0;
                true
            } else {
                ist.consecutive_failures += 1;
                false
            }
        }
        Ok(Err(e)) => {
            let error_string = e.to_string();
            let _ = database::insert_probe_result(
                &state.db,
                &instance.name,
                None,
                None,
                false,
                Some(&error_string),
            )
            .await;

            let mut states = state.instance_states.write().await;
            let ist = states.entry(instance.name.clone()).or_default();
            ist.last_check = Some(now_epoch());
            ist.last_response_time_ms = None;
            ist.last_status_code = None;
            ist.consecutive_failures += 1;

            tracing::warn!("Probe failed for {}: {}", instance.name, error_string);
            false
        }
        Err(_) => {
            let error_string = "timeout".to_string();
            let _ = database::insert_probe_result(
                &state.db,
                &instance.name,
                None,
                None,
                false,
                Some(&error_string),
            )
            .await;

            let mut states = state.instance_states.write().await;
            let ist = states.entry(instance.name.clone()).or_default();
            ist.last_check = Some(now_epoch());
            ist.last_response_time_ms = None;
            ist.last_status_code = None;
            ist.consecutive_failures += 1;

            tracing::warn!("Probe failed for {}: {}", instance.name, error_string);
            false
        }
    }
}

/// Probe with retry logic: if first probe fails, wait and retry once.
pub async fn probe_with_retry(
    state: &SharedState,
    instance: &InstanceConfig,
    client: &reqwest::Client,
    config: &AppConfig,
) -> bool {
    let success = probe_instance(state, instance, client, config).await;
    if success {
        return true;
    }

    // Read consecutive_failures
    let failures = {
        let states = state.instance_states.read().await;
        states
            .get(&instance.name)
            .map(|s| s.consecutive_failures)
            .unwrap_or(0)
    };

    // First failure — retry after delay
    if failures < config.probe.failure_threshold {
        tokio::time::sleep(std::time::Duration::from_secs(config.probe.retry_delay_seconds)).await;
        let success = probe_instance(state, instance, client, config).await;
        if success {
            return true;
        }
    }

    // Read failures again
    let failures = {
        let states = state.instance_states.read().await;
        states
            .get(&instance.name)
            .map(|s| s.consecutive_failures)
            .unwrap_or(0)
    };

    // Confirmed down
    if failures >= config.probe.failure_threshold {
        let mut states = state.instance_states.write().await;
        let ist = states.entry(instance.name.clone()).or_default();
        let was_online = ist.online;
        ist.online = Some(false);
        ist.uptime_since = None;
        if was_online != Some(false) {
            let _ = database::insert_event(
                &state.db,
                "down",
                &format!(
                    "实例 {} 确认宕机（连续 {} 次探测失败）",
                    instance.name, ist.consecutive_failures
                ),
                Some(&instance.name),
                None,
            )
            .await;
        }
    }

    false
}

/// Run one full probe cycle for all enabled instances.
pub async fn run_probe_cycle(state: &SharedState) {
    let config = state.config.get().await;
    let client = reqwest::Client::new();

    let mut handles = Vec::new();

    for inst in &config.instances {
        if !inst.enabled {
            continue;
        }

        let lifecycle = database::get_instance_lifecycle(&state.db, &inst.name)
            .await
            .unwrap_or_default();
        let desired = lifecycle
            .get("desired_state")
            .and_then(|v: &serde_json::Value| v.as_str())
            .unwrap_or("running");

        if desired != "running" {
            let mut states = state.instance_states.write().await;
            let ist = states.entry(inst.name.clone()).or_default();
            ist.online = Some(false);
            ist.uptime_since = None;
            ist.consecutive_failures = 0;
            continue;
        }

        let s = state.clone();
        let i = inst.clone();
        let c = client.clone();
        let cfg = config.clone();
        handles.push(tokio::spawn(async move {
            probe_with_retry(&s, &i, &c, &cfg).await;
        }));
    }

    for h in handles {
        let _ = h.await;
    }
}
