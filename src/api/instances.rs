use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde_json::{json, Value};

use crate::collector;
use crate::config::SharedState;
use crate::database;
use crate::restart;

fn format_uptime(uptime_since: Option<f64>) -> Value {
    match uptime_since {
        Some(ts) => {
            let now = crate::config::now_epoch();
            let seconds = (now - ts) as u64;
            if seconds < 60 {
                json!(format!("{seconds}s"))
            } else {
                let minutes = seconds / 60;
                if minutes < 60 {
                    json!(format!("{minutes}m {}s", seconds % 60))
                } else {
                    let hours = minutes / 60;
                    if hours < 24 {
                        json!(format!("{hours}h {}m", minutes % 60))
                    } else {
                        let days = hours / 24;
                        json!(format!("{days}d {}h", hours % 24))
                    }
                }
            }
        }
        None => Value::Null,
    }
}

pub async fn collect_instances_payload(state: &SharedState) -> Vec<Value> {
    let config = state.config.get().await;
    let all_info = collector::collect_all_info(&config);
    let info_map: std::collections::HashMap<String, Value> = all_info
        .into_iter()
        .filter_map(|v| {
            let name = v.get("name").and_then(|n| n.as_str()).map(|n| n.to_string());
            name.map(|n| (n, v))
        })
        .collect();

    let mut instances = Vec::new();
    for inst in &config.instances {
        if !inst.enabled {
            continue;
        }

        let ist = {
            let states = state.instance_states.read().await;
            states.get(&inst.name).cloned().unwrap_or_default()
        };
        let info = info_map.get(&inst.name).cloned().unwrap_or_else(|| json!({}));
        let lifecycle: Value = database::get_instance_lifecycle(&state.db, &inst.name)
            .await
            .unwrap_or_default();
        let restarts: Vec<Value> = database::get_restart_events(&state.db, Some(&inst.name), None)
            .await
            .unwrap_or_default();
        let last_restart = restarts.first().cloned();

        let desired_state = lifecycle
            .get("desired_state")
            .and_then(|v: &Value| v.as_str())
            .unwrap_or("running");

        let current_state = if ist.online == Some(true) {
            "running"
        } else if desired_state == "stopped" {
            "stopped"
        } else if ist.online == Some(false) {
            "degraded"
        } else {
            "unknown"
        };

        instances.push(json!({
            "name": inst.name,
            "port": inst.port,
            "profile": inst.profile,
            "enabled": inst.enabled,
            "online": ist.online,
            "uptime_since": ist.uptime_since,
            "uptime": format_uptime(ist.uptime_since),
            "last_check": ist.last_check,
            "last_response_time_ms": ist.last_response_time_ms,
            "last_status_code": ist.last_status_code,
            "consecutive_failures": ist.consecutive_failures,
            "default_model": info.get("default_model"),
            "agent_count": info.get("agent_count").and_then(|v| v.as_i64()).unwrap_or(0),
            "agents": info.get("agents").unwrap_or(&json!([])),
            "channels": info.get("channels").unwrap_or(&json!([])),
            "channel_count": info.get("channel_count").and_then(|v| v.as_i64()).unwrap_or(0),
            "enabled_channel_names": info.get("enabled_channel_names").unwrap_or(&json!([])),
            "enabled_channel_count": info.get("enabled_channel_count").and_then(|v| v.as_i64()).unwrap_or(0),
            "crons": info.get("crons").unwrap_or(&json!([])),
            "cron_count": info.get("cron_count").and_then(|v| v.as_i64()).unwrap_or(0),
            "enabled_cron_count": info.get("enabled_cron_count").and_then(|v| v.as_i64()).unwrap_or(0),
            "config_found": info.get("config_found").and_then(|v| v.as_bool()).unwrap_or(false),
            "last_restart": last_restart,
            "lifecycle": {
                "desired_state": desired_state,
                "current_state": current_state,
                "updated_at": lifecycle.get("updated_at"),
                "last_action": lifecycle.get("last_action"),
                "last_action_status": lifecycle.get("last_action_status"),
                "last_action_at": lifecycle.get("last_action_at"),
                "last_action_message": lifecycle.get("last_action_message"),
                "auto_restart_enabled": desired_state == "running",
            },
        }));
    }

    instances
}

fn get_instance_or_404(
    config: &crate::config::AppConfig,
    name: &str,
) -> Result<crate::config::InstanceConfig, (StatusCode, Json<Value>)> {
    config
        .instances
        .iter()
        .find(|i| i.name == name)
        .cloned()
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(json!({"detail": format!("Instance '{}' not found", name)})),
            )
        })
}

// GET /api/instances
pub async fn list_instances(State(state): State<SharedState>) -> Json<Value> {
    let instances = collect_instances_payload(&state).await;
    Json(json!({ "instances": instances }))
}

// GET /api/instances/:name
pub async fn get_instance(
    State(state): State<SharedState>,
    Path(name): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let config = state.config.get().await;
    let inst = get_instance_or_404(&config, &name)?;

    let ist = {
        let states = state.instance_states.read().await;
        states.get(&inst.name).cloned().unwrap_or_default()
    };
    let info = collector::collect_instance_info(&inst);
    let lifecycle: Value = database::get_instance_lifecycle(&state.db, &inst.name)
        .await
        .unwrap_or_default();
    let probe_history: Vec<Value> = database::get_probe_results(&state.db, &inst.name, 50)
        .await
        .unwrap_or_default();
    let restarts: Vec<Value> = database::get_restart_events(&state.db, Some(&inst.name), None)
        .await
        .unwrap_or_default();

    let desired_state = lifecycle
        .get("desired_state")
        .and_then(|v: &Value| v.as_str())
        .unwrap_or("running");
    let current_state = if ist.online == Some(true) {
        "running"
    } else if desired_state == "stopped" {
        "stopped"
    } else if ist.online == Some(false) {
        "degraded"
    } else {
        "unknown"
    };

    Ok(Json(json!({
        "name": inst.name,
        "port": inst.port,
        "profile": inst.profile,
        "enabled": inst.enabled,
        "online": ist.online,
        "uptime_since": ist.uptime_since,
        "uptime": format_uptime(ist.uptime_since),
        "last_check": ist.last_check,
        "last_response_time_ms": ist.last_response_time_ms,
        "consecutive_failures": ist.consecutive_failures,
        "info": info,
        "channel_count": info.get("channel_count").and_then(|v| v.as_i64()).unwrap_or(0),
        "enabled_channel_names": info.get("enabled_channel_names").unwrap_or(&json!([])),
        "enabled_channel_count": info.get("enabled_channel_count").and_then(|v| v.as_i64()).unwrap_or(0),
        "crons": info.get("crons").unwrap_or(&json!([])),
        "cron_count": info.get("cron_count").and_then(|v| v.as_i64()).unwrap_or(0),
        "enabled_cron_count": info.get("enabled_cron_count").and_then(|v| v.as_i64()).unwrap_or(0),
        "lifecycle": {
            "desired_state": desired_state,
            "current_state": current_state,
            "updated_at": lifecycle.get("updated_at"),
            "last_action": lifecycle.get("last_action"),
            "last_action_status": lifecycle.get("last_action_status"),
            "last_action_at": lifecycle.get("last_action_at"),
            "last_action_message": lifecycle.get("last_action_message"),
            "auto_restart_enabled": desired_state == "running",
        },
        "probe_history": probe_history,
        "restart_history": restarts,
    })))
}

// POST /api/instances/:name/restart
pub async fn restart_instance_api(
    State(state): State<SharedState>,
    Path(name): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let config = state.config.get().await;
    let inst = get_instance_or_404(&config, &name)?;
    let result = restart::manual_restart(&state, &inst).await;
    Ok(Json(result))
}
