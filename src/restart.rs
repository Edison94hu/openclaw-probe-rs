use crate::config::{now_epoch, AppConfig, InstanceConfig, SharedState};
use crate::database;
use tokio::process::Command;

/// Check if we should attempt a restart (within rate limit window).
pub async fn should_restart(state: &SharedState, instance: &InstanceConfig) -> bool {
    let config = state.config.get().await;
    if !config.restart.enabled {
        return false;
    }

    let lifecycle: serde_json::Value = database::get_instance_lifecycle(&state.db, &instance.name)
        .await
        .unwrap_or_default();
    let desired = lifecycle
        .get("desired_state")
        .and_then(|v: &serde_json::Value| v.as_str())
        .unwrap_or("running");
    if desired != "running" {
        return false;
    }

    let recent: Vec<serde_json::Value> = database::get_restart_events(
        &state.db,
        Some(&instance.name),
        Some(config.restart.window_minutes),
    )
    .await
    .unwrap_or_default();

    if recent.len() as u32 >= config.restart.max_restarts_per_window {
        return false;
    }
    true
}

fn build_gateway_command(action: &str, instance: &InstanceConfig) -> Vec<String> {
    let mut cmd = vec![
        "openclaw".to_string(),
        "gateway".to_string(),
        action.to_string(),
    ];
    if let Some(profile) = instance.profile {
        cmd.push("--profile".to_string());
        cmd.push(profile.to_string());
    }
    cmd
}

async fn check_instance_health(instance: &InstanceConfig, config: &AppConfig) -> bool {
    let url = format!("http://127.0.0.1:{}/health", instance.port);
    let client = reqwest::Client::new();
    match tokio::time::timeout(
        std::time::Duration::from_secs(config.probe.timeout_seconds),
        client.get(&url).send(),
    )
    .await
    {
        Ok(Ok(resp)) => (200..300).contains(&resp.status().as_u16()),
        _ => false,
    }
}

fn mark_instance_online(state: &mut crate::config::InstanceState) {
    state.online = Some(true);
    state.consecutive_failures = 0;
    state.uptime_since = Some(now_epoch());
    state.last_check = Some(now_epoch());
}

fn mark_instance_stopped(state: &mut crate::config::InstanceState) {
    state.online = Some(false);
    state.consecutive_failures = 0;
    state.uptime_since = None;
    state.last_check = Some(now_epoch());
    state.last_response_time_ms = None;
    state.last_status_code = None;
}

/// Core gateway action runner.
async fn run_gateway_action(
    app: &SharedState,
    instance: &InstanceConfig,
    action: &str,
    trigger_reason: &str,
    desired_state: &str,
    wait_seconds: Option<u64>,
) -> serde_json::Value {
    let config = app.config.get().await;
    let cmd_parts = build_gateway_command(action, instance);
    let wait = wait_seconds.unwrap_or(config.restart.wait_after_restart_seconds);

    let result = Command::new(&cmd_parts[0])
        .args(&cmd_parts[1..])
        .output()
        .await;

    match result {
        Ok(output) => {
            let exit_code = output.status.code().unwrap_or(-1);
            let stdout_text = String::from_utf8_lossy(&output.stdout).trim().to_string();
            let stderr_text = String::from_utf8_lossy(&output.stderr).trim().to_string();
            let output_text = if stderr_text.is_empty() {
                &stdout_text
            } else {
                &stderr_text
            };
            let mut health_after: Option<bool> = None;
            let mut success = exit_code == 0;

            if success {
                tokio::time::sleep(std::time::Duration::from_secs(wait)).await;
                if action == "stop" {
                    let h = check_instance_health(instance, &config).await;
                    health_after = Some(h);
                    success = !h;
                    if success {
                        let mut states = app.instance_states.write().await;
                        let ist = states.entry(instance.name.clone()).or_default();
                        mark_instance_stopped(ist);
                    }
                } else {
                    let h = check_instance_health(instance, &config).await;
                    health_after = Some(h);
                    success = h;
                    if success {
                        let mut states = app.instance_states.write().await;
                        let ist = states.entry(instance.name.clone()).or_default();
                        mark_instance_online(ist);
                    }
                }
            }

            let status_str = if success { "success" } else { "failed" };
            let _ = database::record_instance_lifecycle_action(
                &app.db,
                &instance.name,
                action,
                status_str,
                Some(output_text),
            )
            .await;

            if action == "restart" {
                let _ = database::insert_restart_event(
                    &app.db,
                    &instance.name,
                    trigger_reason,
                    success,
                    Some(exit_code),
                    if stderr_text.is_empty() {
                        None
                    } else {
                        Some(&stderr_text)
                    },
                    health_after,
                )
                .await;
            }

            serde_json::json!({
                "success": success,
                "exit_code": exit_code,
                "stdout": if stdout_text.is_empty() { None } else { Some(&stdout_text) },
                "stderr": if stderr_text.is_empty() { None } else { Some(&stderr_text) },
                "message": output_text,
                "health_after": health_after,
                "desired_state": desired_state,
            })
        }
        Err(e) => {
            let message = if e.kind() == std::io::ErrorKind::NotFound {
                "openclaw 命令未找到".to_string()
            } else {
                e.to_string()
            };
            let _ = database::record_instance_lifecycle_action(
                &app.db,
                &instance.name,
                action,
                "failed",
                Some(&message),
            )
            .await;
            serde_json::json!({
                "success": false,
                "error": message,
                "desired_state": desired_state,
            })
        }
    }
}

/// Automatic restart triggered by probe loop.
pub async fn restart_instance(state: &SharedState, instance: &InstanceConfig) -> bool {
    let config = state.config.get().await;

    if !should_restart(state, instance).await {
        let msg = format!(
            "实例 {} 在 {} 分钟内已重启 {} 次，停止自动重启",
            instance.name, config.restart.window_minutes, config.restart.max_restarts_per_window
        );
        tracing::warn!("{}", msg);
        let _ =
            database::insert_event(&state.db, "restart_limit", &msg, Some(&instance.name), None)
                .await;
        return false;
    }

    let result = run_gateway_action(
        state,
        instance,
        "restart",
        "auto_restart",
        "running",
        None,
    )
    .await;

    let success = result.get("success").and_then(|v| v.as_bool()).unwrap_or(false);
    let health_after = result.get("health_after").and_then(|v| v.as_bool());

    if success {
        let _ = database::insert_event(
            &state.db,
            "restart_success",
            &format!("实例 {} 重启成功并已恢复在线", instance.name),
            Some(&instance.name),
            None,
        )
        .await;
    } else {
        let err = result
            .get("stderr")
            .or_else(|| result.get("error"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let _ = database::insert_event(
            &state.db,
            "restart_failed",
            &format!("实例 {} 重启失败", instance.name),
            Some(&instance.name),
            if err.is_empty() { None } else { Some(err) },
        )
        .await;
    }

    health_after.unwrap_or(false)
}

/// Manually trigger a restart (bypasses rate limit).
pub async fn manual_restart(state: &SharedState, instance: &InstanceConfig) -> serde_json::Value {
    let _ = database::set_instance_desired_state(
        &state.db,
        &instance.name,
        "running",
        "restart",
        "accepted",
        Some("Manual restart requested"),
    )
    .await;

    let result = run_gateway_action(
        state,
        instance,
        "restart",
        "manual_restart",
        "running",
        None,
    )
    .await;

    let success = result.get("success").and_then(|v| v.as_bool()).unwrap_or(false);
    let health_str = if result
        .get("health_after")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        "通过"
    } else {
        "未通过"
    };
    let event_type = if success {
        "restart_success"
    } else {
        "restart_failed"
    };
    let _ = database::insert_event(
        &state.db,
        event_type,
        &format!(
            "手动重启实例 {}，健康检查: {}",
            instance.name, health_str
        ),
        Some(&instance.name),
        result.get("stderr").and_then(|v| v.as_str()),
    )
    .await;

    result
}

/// Manually start an instance.
pub async fn manual_start(state: &SharedState, instance: &InstanceConfig) -> serde_json::Value {
    let _ = database::set_instance_desired_state(
        &state.db,
        &instance.name,
        "running",
        "start",
        "accepted",
        Some("Manual start requested"),
    )
    .await;

    let result =
        run_gateway_action(state, instance, "start", "manual_start", "running", None).await;

    let success = result.get("success").and_then(|v| v.as_bool()).unwrap_or(false);
    let event_type = if success {
        "start_success"
    } else {
        "start_failed"
    };
    let _ = database::insert_event(
        &state.db,
        event_type,
        &format!("手动启动实例 {}", instance.name),
        Some(&instance.name),
        result
            .get("message")
            .or_else(|| result.get("error"))
            .and_then(|v| v.as_str()),
    )
    .await;

    result
}

/// Manually stop an instance.
pub async fn manual_stop(state: &SharedState, instance: &InstanceConfig) -> serde_json::Value {
    let _ = database::set_instance_desired_state(
        &state.db,
        &instance.name,
        "stopped",
        "stop",
        "accepted",
        Some("Manual stop requested"),
    )
    .await;

    let result =
        run_gateway_action(state, instance, "stop", "manual_stop", "stopped", Some(5)).await;

    let success = result.get("success").and_then(|v| v.as_bool()).unwrap_or(false);
    let event_type = if success {
        "stop_success"
    } else {
        "stop_failed"
    };
    let _ = database::insert_event(
        &state.db,
        event_type,
        &format!("手动停止实例 {}", instance.name),
        Some(&instance.name),
        result
            .get("message")
            .or_else(|| result.get("error"))
            .and_then(|v| v.as_str()),
    )
    .await;

    result
}
