use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde_json::{json, Value};
use std::sync::Mutex;
use std::time::Instant;

use crate::config::{now_epoch, SharedState};
use crate::database;
use crate::operations;
use crate::restart;
use crate::system_metrics;

use super::instances::collect_instances_payload;
use super::tokens::{build_agent_heatmap_payload, build_token_summary_payload};

// ---------------------------------------------------------------------------
// Caches
// ---------------------------------------------------------------------------

struct CacheEntry {
    value: Value,
    cached_at: f64,
    build_ms: f64,
}

static PAYLOAD_CACHE: Mutex<Option<std::collections::HashMap<String, CacheEntry>>> =
    Mutex::new(None);

fn get_cached_or_build<'a>(
    key: &str,
    ttl_seconds: f64,
) -> Option<(Value, Value)> {
    let now = now_epoch();
    let guard = PAYLOAD_CACHE.lock().unwrap();
    if let Some(ref cache_map) = *guard {
        if let Some(entry) = cache_map.get(key) {
            if now - entry.cached_at < ttl_seconds {
                let meta = json!({
                    "cache_hit": true,
                    "cache_age_ms": ((now - entry.cached_at) * 1000.0 * 100.0).round() / 100.0,
                    "ttl_ms": ttl_seconds * 1000.0,
                    "build_ms": entry.build_ms,
                    "stale_fallback": false,
                });
                return Some((entry.value.clone(), meta));
            }
        }
    }
    None
}

fn set_cache(key: &str, value: Value, build_ms: f64) {
    let now = now_epoch();
    let mut guard = PAYLOAD_CACHE.lock().unwrap();
    let cache_map = guard.get_or_insert_with(std::collections::HashMap::new);
    cache_map.insert(
        key.to_string(),
        CacheEntry {
            value,
            cached_at: now,
            build_ms,
        },
    );
}

async fn cached_payload(
    key: &str,
    ttl_seconds: f64,
    builder: impl std::future::Future<Output = Value>,
) -> (Value, Value) {
    if let Some(cached) = get_cached_or_build(key, ttl_seconds) {
        return cached;
    }

    let start = Instant::now();
    let value = builder.await;
    let build_ms = (start.elapsed().as_secs_f64() * 1000.0 * 100.0).round() / 100.0;
    set_cache(key, value.clone(), build_ms);

    let meta = json!({
        "cache_hit": false,
        "cache_age_ms": 0.0,
        "ttl_ms": ttl_seconds * 1000.0,
        "build_ms": build_ms,
        "stale_fallback": false,
    });
    (value, meta)
}

async fn build_host_meta(config: &crate::config::AppConfig) -> Value {
    let install_info = operations::get_openclaw_install_info().await;
    json!({
        "agent_name": config.agent.name,
        "site": config.agent.site,
        "hostname": hostname::get().map(|h| h.to_string_lossy().into_owned()).unwrap_or_default(),
        "service_version": "1.0.0",
        "openclaw_version": install_info.get("openclaw_version"),
        "openclaw_version_full": install_info.get("openclaw_version_full"),
        "server_time": now_epoch(),
    })
}

fn build_probe_timing(instances: &[Value]) -> Value {
    let mut per_instance = serde_json::Map::new();
    for item in instances {
        if let (Some(name), Some(ms)) = (
            item.get("name").and_then(|v| v.as_str()),
            item.get("last_response_time_ms").and_then(|v| v.as_f64()),
        ) {
            per_instance.insert(name.to_string(), json!(ms));
        }
    }
    let values: Vec<f64> = per_instance.values().filter_map(|v| v.as_f64()).collect();
    let avg = if values.is_empty() {
        Value::Null
    } else {
        json!((values.iter().sum::<f64>() / values.len() as f64 * 100.0).round() / 100.0)
    };
    let max = values.iter().copied().reduce(f64::max).map(|v| json!(v)).unwrap_or(Value::Null);

    json!({
        "openclaw_probe_ms_by_instance": per_instance,
        "openclaw_probe_avg_ms": avg,
        "openclaw_probe_max_ms": max,
    })
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

// ---------------------------------------------------------------------------
// Endpoints
// ---------------------------------------------------------------------------

// GET /api/health
pub async fn probe_health() -> Json<Value> {
    Json(json!({
        "status": "ok",
        "service": "openclaw-probe",
    }))
}

// GET /api/agent/health
pub async fn agent_health(State(state): State<SharedState>) -> Json<Value> {
    let start = Instant::now();
    let config = state.config.get().await;
    let instances = collect_instances_payload(&state).await;
    let maintenance: bool = database::get_maintenance_mode(&state.db)
        .await
        .unwrap_or(false);

    let processing_ms = (start.elapsed().as_secs_f64() * 1000.0 * 100.0).round() / 100.0;
    let mut timing = build_probe_timing(&instances);
    if let Some(obj) = timing.as_object_mut() {
        obj.insert("api_processing_ms".into(), json!(processing_ms));
        obj.insert("generated_at".into(), json!(now_epoch()));
    }

    let uptime = state.started_at.elapsed().as_secs_f64();
    Json(json!({
        "status": "ok",
        "service": "openclaw-probe-agent",
        "process_uptime_seconds": (uptime * 100.0).round() / 100.0,
        "host": build_host_meta(&config).await,
        "system": system_metrics::get_system_metrics(false),
        "maintenance_mode": maintenance,
        "instances_total": instances.len(),
        "instances_online": instances.iter().filter(|i| i.get("online") == Some(&json!(true))).count(),
        "timing": timing,
    }))
}

// GET /api/agent/snapshot
pub async fn agent_snapshot(State(state): State<SharedState>) -> Json<Value> {
    let start = Instant::now();
    let config = state.config.get().await;

    let instances = collect_instances_payload(&state).await;
    let maintenance: bool = database::get_maintenance_mode(&state.db)
        .await
        .unwrap_or(false);
    let openclaw_install = operations::get_openclaw_install_info().await;

    let (openclaw_release, openclaw_release_meta) =
        cached_payload("openclaw_release", 300.0, operations::get_openclaw_latest_release(false))
            .await;
    let (token_summary, token_summary_meta) = cached_payload(
        "token_summary",
        15.0,
        build_token_summary_payload(&state),
    )
    .await;
    let (agent_heatmap, agent_heatmap_meta) = cached_payload(
        "agent_heatmap:today",
        30.0,
        build_agent_heatmap_payload(&state, "today"),
    )
    .await;
    let (recent_events, recent_events_meta) = cached_payload("recent_events:50", 5.0, async {
        let events: Vec<Value> = database::get_recent_events(&state.db, 50, 0, None)
            .await
            .unwrap_or_default();
        json!(events)
    })
    .await;

    let processing_ms = (start.elapsed().as_secs_f64() * 1000.0 * 100.0).round() / 100.0;
    let mut timing = build_probe_timing(&instances);
    if let Some(obj) = timing.as_object_mut() {
        obj.insert("api_processing_ms".into(), json!(processing_ms));
        obj.insert("generated_at".into(), json!(now_epoch()));
        obj.insert(
            "cached_sections".into(),
            json!({
                "openclaw_release": openclaw_release_meta,
                "token_summary": token_summary_meta,
                "agent_heatmap": agent_heatmap_meta,
                "events": recent_events_meta,
            }),
        );
    }

    Json(json!({
        "status": "ok",
        "service": "openclaw-probe-agent",
        "host": build_host_meta(&config).await,
        "system": system_metrics::get_system_metrics(false),
        "config": {
            "probe": serde_json::to_value(&config.probe).unwrap_or_default(),
            "restart": serde_json::to_value(&config.restart).unwrap_or_default(),
            "token_scan": serde_json::to_value(&config.token_scan).unwrap_or_default(),
            "operations": serde_json::to_value(&config.operations).unwrap_or_default(),
        },
        "maintenance_mode": maintenance,
        "openclaw_install": openclaw_install,
        "openclaw_release": openclaw_release,
        "instances": instances,
        "token_summary": token_summary,
        "agent_heatmap": agent_heatmap,
        "events": recent_events,
        "timing": timing,
    }))
}

// POST /api/agent/instances/:name/restart
pub async fn agent_restart_instance(
    State(state): State<SharedState>,
    Path(name): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let config = state.config.get().await;
    let inst = get_instance_or_404(&config, &name)?;
    let result = restart::manual_restart(&state, &inst).await;
    let success = result
        .get("success")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let mut resp = json!({
        "status": if success { "ok" } else { "error" },
        "instance_name": name,
    });
    if let (Some(resp_obj), Some(result_obj)) = (resp.as_object_mut(), result.as_object()) {
        for (k, v) in result_obj {
            resp_obj.insert(k.clone(), v.clone());
        }
    }
    Ok(Json(resp))
}

// POST /api/agent/instances/:name/start
pub async fn agent_start_instance(
    State(state): State<SharedState>,
    Path(name): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let config = state.config.get().await;
    let inst = get_instance_or_404(&config, &name)?;
    let result = restart::manual_start(&state, &inst).await;
    let success = result
        .get("success")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let mut resp = json!({
        "status": if success { "ok" } else { "error" },
        "instance_name": name,
    });
    if let (Some(resp_obj), Some(result_obj)) = (resp.as_object_mut(), result.as_object()) {
        for (k, v) in result_obj {
            resp_obj.insert(k.clone(), v.clone());
        }
    }
    Ok(Json(resp))
}

// POST /api/agent/instances/:name/stop
pub async fn agent_stop_instance(
    State(state): State<SharedState>,
    Path(name): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let config = state.config.get().await;
    let inst = get_instance_or_404(&config, &name)?;
    let result = restart::manual_stop(&state, &inst).await;
    let success = result
        .get("success")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let mut resp = json!({
        "status": if success { "ok" } else { "error" },
        "instance_name": name,
    });
    if let (Some(resp_obj), Some(result_obj)) = (resp.as_object_mut(), result.as_object()) {
        for (k, v) in result_obj {
            resp_obj.insert(k.clone(), v.clone());
        }
    }
    Ok(Json(resp))
}

// POST /api/agent/instances/:name/desired-state
pub async fn agent_set_desired_state(
    State(state): State<SharedState>,
    Path(name): Path<String>,
    Json(payload): Json<Value>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let config = state.config.get().await;
    let inst = get_instance_or_404(&config, &name)?;

    let desired_state = payload
        .get("desired_state")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_lowercase();

    if desired_state != "running" && desired_state != "stopped" {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"detail": "desired_state must be 'running' or 'stopped'"})),
        ));
    }

    let _ = database::set_instance_desired_state(
        &state.db,
        &inst.name,
        &desired_state,
        "desired_state",
        "accepted",
        Some(&format!("Desired state updated to {desired_state}")),
    )
    .await;

    let lifecycle: Value = database::get_instance_lifecycle(&state.db, &inst.name)
        .await
        .unwrap_or_default();

    Ok(Json(json!({
        "status": "ok",
        "instance_name": name,
        "lifecycle": lifecycle,
    })))
}

// GET /api/agent/instances/:name/lifecycle
pub async fn agent_get_instance_lifecycle(
    State(state): State<SharedState>,
    Path(name): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let config = state.config.get().await;
    let inst = get_instance_or_404(&config, &name)?;
    let lifecycle: Value = database::get_instance_lifecycle(&state.db, &inst.name)
        .await
        .unwrap_or_default();
    Ok(Json(json!({
        "status": "ok",
        "instance_name": name,
        "lifecycle": lifecycle,
    })))
}

// POST /api/agent/maintenance/enter
pub async fn agent_enter_maintenance(
    State(state): State<SharedState>,
    Json(payload): Json<Value>,
) -> Json<Value> {
    let reason = payload
        .get("reason")
        .and_then(|v| v.as_str())
        .map(|s| s.trim())
        .filter(|s| !s.is_empty());
    let result = operations::enter_maintenance(&state, reason).await;
    let mut resp = json!({"status": "ok"});
    if let (Some(resp_obj), Some(result_obj)) = (resp.as_object_mut(), result.as_object()) {
        for (k, v) in result_obj {
            resp_obj.insert(k.clone(), v.clone());
        }
    }
    Json(resp)
}

// POST /api/agent/maintenance/exit
pub async fn agent_exit_maintenance(
    State(state): State<SharedState>,
    Json(payload): Json<Value>,
) -> Json<Value> {
    let reason = payload
        .get("reason")
        .and_then(|v| v.as_str())
        .map(|s| s.trim())
        .filter(|s| !s.is_empty());
    let result = operations::exit_maintenance(&state, reason).await;
    let mut resp = json!({"status": "ok"});
    if let (Some(resp_obj), Some(result_obj)) = (resp.as_object_mut(), result.as_object()) {
        for (k, v) in result_obj {
            resp_obj.insert(k.clone(), v.clone());
        }
    }
    Json(resp)
}

// GET /api/agent/openclaw/version
pub async fn agent_openclaw_version() -> Json<Value> {
    Json(json!({
        "status": "ok",
        "openclaw": operations::get_openclaw_install_info().await,
    }))
}

// GET /api/agent/openclaw/latest-release
pub async fn agent_openclaw_latest_release() -> Json<Value> {
    Json(json!({
        "status": "ok",
        "release": operations::get_openclaw_latest_release(false).await,
    }))
}

// GET /api/agent/tasks
pub async fn agent_list_tasks() -> Json<Value> {
    Json(json!({
        "status": "ok",
        "tasks": operations::list_tasks(20).await,
    }))
}

// GET /api/agent/tasks/:task_id
pub async fn agent_get_task(Path(task_id): Path<String>) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    match operations::get_task(&task_id).await {
        Some(task) => Ok(Json(json!({ "status": "ok", "task": task }))),
        None => Err((
            StatusCode::NOT_FOUND,
            Json(json!({"detail": format!("Task '{}' not found", task_id)})),
        )),
    }
}

// GET /api/agent/backups
pub async fn agent_list_backups(State(state): State<SharedState>) -> Json<Value> {
    Json(json!({
        "status": "ok",
        "backups": operations::list_backups(&state).await,
    }))
}

// GET /api/agent/backups/:backup_id
pub async fn agent_get_backup(
    State(state): State<SharedState>,
    Path(backup_id): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    match operations::get_backup(&state, &backup_id).await {
        Some(backup) => Ok(Json(json!({ "status": "ok", "backup": backup }))),
        None => Err((
            StatusCode::NOT_FOUND,
            Json(json!({"detail": format!("Backup '{}' not found", backup_id)})),
        )),
    }
}

// POST /api/agent/backups/create
pub async fn agent_create_backup(
    State(state): State<SharedState>,
    Json(payload): Json<Value>,
) -> Json<Value> {
    let label = payload
        .get("label")
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let task = operations::create_backup_task(state, label).await;
    Json(json!({ "status": "accepted", "task": task }))
}

// POST /api/agent/backups/:backup_id/restore
pub async fn agent_restore_backup(
    State(state): State<SharedState>,
    Path(backup_id): Path<String>,
    Json(payload): Json<Value>,
) -> Json<Value> {
    let start_after_restore = payload
        .get("start_after_restore")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let task = operations::create_restore_task(state, backup_id, start_after_restore).await;
    Json(json!({ "status": "accepted", "task": task }))
}

// POST /api/agent/openclaw/upgrade
pub async fn agent_upgrade_openclaw(
    State(state): State<SharedState>,
    Json(payload): Json<Value>,
) -> Json<Value> {
    let target_version = payload
        .get("target_version")
        .and_then(|v| v.as_str())
        .unwrap_or("latest")
        .trim()
        .to_string();
    let create_backup = payload
        .get("create_backup")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let rollback_on_failure = payload
        .get("rollback_on_failure")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);

    let task = operations::create_upgrade_task(
        state,
        target_version,
        create_backup,
        rollback_on_failure,
    )
    .await;
    Json(json!({ "status": "accepted", "task": task }))
}

// POST /api/agent/openclaw/upgrade-if-needed
pub async fn agent_upgrade_if_needed(
    State(state): State<SharedState>,
    Json(payload): Json<Value>,
) -> Json<Value> {
    let create_backup = payload
        .get("create_backup")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let rollback_on_failure = payload
        .get("rollback_on_failure")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let force_refresh_release = payload
        .get("force_refresh_release")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);

    let task = operations::create_upgrade_if_needed_task(
        state,
        create_backup,
        rollback_on_failure,
        force_refresh_release,
    )
    .await;
    Json(json!({ "status": "accepted", "task": task }))
}
