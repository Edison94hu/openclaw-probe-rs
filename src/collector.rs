use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::UNIX_EPOCH;

use crate::config::{expand_home, AppConfig, InstanceConfig};

// ---------------------------------------------------------------------------
// Cache
// ---------------------------------------------------------------------------

type CacheSignature = (Option<u64>, Option<u64>, Vec<(String, Option<u64>)>);

static INFO_CACHE: Mutex<Option<HashMap<String, (CacheSignature, Value)>>> = Mutex::new(None);

fn get_cache() -> &'static Mutex<Option<HashMap<String, (CacheSignature, Value)>>> {
    &INFO_CACHE
}

fn file_mtime_millis(path: &Path) -> Option<u64> {
    std::fs::metadata(path)
        .ok()?
        .modified()
        .ok()?
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|d| d.as_millis() as u64)
}

fn file_mtime_secs_f64(path: &Path) -> Option<f64> {
    std::fs::metadata(path)
        .ok()?
        .modified()
        .ok()?
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|d| d.as_secs_f64())
}

// ---------------------------------------------------------------------------
// Soul file helpers
// ---------------------------------------------------------------------------

fn resolve_soul_file(workspace: Option<&str>) -> Option<PathBuf> {
    let ws = workspace?;
    let base = PathBuf::from(shellexpand_home(ws));
    for name in &["SOUL.md", "soul.md"] {
        let candidate = base.join(name);
        if candidate.exists() {
            return Some(candidate);
        }
    }
    None
}

fn shellexpand_home(s: &str) -> String {
    if let Some(rest) = s.strip_prefix("~/") {
        if let Ok(home) = std::env::var("HOME") {
            return format!("{home}/{rest}");
        }
    }
    s.to_string()
}

fn read_soul_file(path: Option<&Path>) -> Value {
    let path = match path {
        Some(p) => p,
        None => {
            return json!({
                "exists": false, "path": null, "title": null,
                "content": null, "preview": null, "updated_at": null,
            });
        }
    };

    match std::fs::read_to_string(path) {
        Ok(content) => {
            let lines: Vec<&str> = content
                .lines()
                .map(|l| l.trim())
                .filter(|l| !l.is_empty())
                .collect();
            let title = lines.first().copied().unwrap_or("").to_string();
            let preview = lines.iter().take(8).copied().collect::<Vec<_>>().join("\n");
            json!({
                "exists": true,
                "path": path.to_string_lossy(),
                "title": title,
                "content": content,
                "preview": preview,
                "updated_at": file_mtime_secs_f64(path),
            })
        }
        Err(e) => {
            json!({
                "exists": false,
                "path": path.to_string_lossy(),
                "title": path.file_name().map(|n| n.to_string_lossy().to_string()),
                "content": null, "preview": null, "updated_at": null,
                "error": e.to_string(),
            })
        }
    }
}

// ---------------------------------------------------------------------------
// Cron jobs
// ---------------------------------------------------------------------------

fn read_cron_jobs(home: &Path) -> (Vec<Value>, HashMap<String, Vec<Value>>) {
    let cron_jobs_file = home.join("cron").join("jobs.json");
    let cron_runs_dir = home.join("cron").join("runs");

    if !cron_jobs_file.exists() {
        return (vec![], HashMap::new());
    }

    let payload: Value = match std::fs::read_to_string(&cron_jobs_file)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
    {
        Some(v) => v,
        None => return (vec![], HashMap::new()),
    };

    let jobs = match payload.get("jobs").and_then(|v| v.as_array()) {
        Some(arr) => arr.clone(),
        None => return (vec![], HashMap::new()),
    };

    let mut normalized_jobs = Vec::new();
    let mut jobs_by_agent: HashMap<String, Vec<Value>> = HashMap::new();

    for job in &jobs {
        let obj = match job.as_object() {
            Some(o) => o,
            None => continue,
        };

        let job_id = obj.get("id").and_then(|v| v.as_str()).unwrap_or("");

        // Read latest run
        let mut latest_run = Value::Null;
        if !job_id.is_empty() {
            let run_file = cron_runs_dir.join(format!("{job_id}.jsonl"));
            if run_file.exists() {
                if let Ok(content) = std::fs::read_to_string(&run_file) {
                    if let Some(last_line) = content.lines().rev().find(|l| !l.trim().is_empty()) {
                        if let Ok(v) = serde_json::from_str::<Value>(last_line) {
                            latest_run = v;
                        }
                    }
                }
            }
        }

        let payload_obj = obj.get("payload").and_then(|v| v.as_object());
        let schedule = obj
            .get("schedule")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let state_obj = obj.get("state").cloned().unwrap_or_else(|| json!({}));
        let enabled = obj.get("enabled").and_then(|v| v.as_bool()).unwrap_or(true);

        let run_file_path = if !job_id.is_empty() {
            Some(cron_runs_dir.join(format!("{job_id}.jsonl")).to_string_lossy().to_string())
        } else {
            None
        };

        let agent_id_val = obj.get("agentId").and_then(|v| v.as_str()).map(|s| s.to_string());

        let mut normalized = json!({
            "id": obj.get("id"),
            "agent_id": agent_id_val,
            "name": obj.get("name").and_then(|v| v.as_str()).or_else(|| obj.get("id").and_then(|v| v.as_str())),
            "enabled": enabled,
            "delete_after_run": obj.get("deleteAfterRun").and_then(|v| v.as_bool()).unwrap_or(false),
            "created_at_ms": obj.get("createdAtMs"),
            "updated_at_ms": obj.get("updatedAtMs"),
            "schedule": schedule.clone(),
            "session_target": obj.get("sessionTarget"),
            "wake_mode": obj.get("wakeMode"),
            "delivery": obj.get("delivery").cloned().unwrap_or_else(|| json!({})),
            "payload": {
                "kind": payload_obj.and_then(|p| p.get("kind")),
                "message": payload_obj.and_then(|p| p.get("message")),
                "text": payload_obj.and_then(|p| p.get("text")),
                "timeout_seconds": payload_obj.and_then(|p| p.get("timeoutSeconds")),
                "model": payload_obj.and_then(|p| p.get("model")),
            },
            "state": state_obj.clone(),
            "latest_run": latest_run.clone(),
            "run_log_path": run_file_path,
        });

        // Build display fields
        let display = build_cron_display_fields(&normalized);
        if let Some(obj_mut) = normalized.as_object_mut() {
            if let Some(display_obj) = display.as_object() {
                for (k, v) in display_obj {
                    obj_mut.insert(k.clone(), v.clone());
                }
            }
        }

        normalized_jobs.push(normalized.clone());

        if let Some(ref aid) = agent_id_val {
            jobs_by_agent
                .entry(aid.clone())
                .or_default()
                .push(normalized);
        }
    }

    (normalized_jobs, jobs_by_agent)
}

fn format_ts_ms(ts_ms: Option<i64>) -> Value {
    match ts_ms {
        Some(ms) if ms > 0 => {
            let secs = ms / 1000;
            let dt = chrono::DateTime::from_timestamp(secs, 0)
                .map(|d| d.with_timezone(&chrono::FixedOffset::east_opt(8 * 3600).unwrap()));
            match dt {
                Some(d) => json!(d.format("%Y-%m-%d %H:%M:%S").to_string()),
                None => Value::Null,
            }
        }
        _ => Value::Null,
    }
}

fn humanize_delta_ms(ts_ms: Option<i64>) -> Value {
    match ts_ms {
        Some(ms) if ms > 0 => {
            let now_ms = std::time::SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;
            let delta = ((ms - now_ms).abs() / 1000) as u64;
            let s = if delta < 60 {
                format!("{delta}s")
            } else if delta < 3600 {
                format!("{}m", delta / 60)
            } else if delta < 86400 {
                format!("{}h", delta / 3600)
            } else {
                format!("{}d", delta / 86400)
            };
            json!(s)
        }
        _ => Value::Null,
    }
}

fn truncate_chars(s: &str, max_chars: usize) -> String {
    s.chars().take(max_chars).collect()
}

fn schedule_label(schedule: &Value) -> String {
    let kind = schedule.get("kind").and_then(|v| v.as_str()).unwrap_or("");
    match kind {
        "cron" => {
            let expr = schedule.get("expr").and_then(|v| v.as_str()).unwrap_or("");
            let tz = schedule.get("tz").and_then(|v| v.as_str());
            match tz {
                Some(tz) => format!("cron: {expr} ({tz})"),
                None => format!("cron: {expr}"),
            }
        }
        "at" => {
            let at = schedule.get("at").and_then(|v| v.as_str()).unwrap_or("");
            format!("at: {at}")
        }
        other => other.to_string(),
    }
}

fn derive_cron_health(enabled: bool, state: &Value) -> &'static str {
    if !enabled {
        return "disabled";
    }
    let last_status = state
        .get("lastStatus")
        .or_else(|| state.get("lastRunStatus"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let consecutive_errors = state
        .get("consecutiveErrors")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);

    if last_status == "ok" {
        "healthy"
    } else if consecutive_errors >= 3 {
        "failing"
    } else if last_status == "error" {
        "warning"
    } else {
        "idle"
    }
}

fn build_cron_display_fields(job: &Value) -> Value {
    let state = job.get("state").unwrap_or(&Value::Null);
    let latest_run = job.get("latest_run").unwrap_or(&Value::Null);
    let enabled = job.get("enabled").and_then(|v| v.as_bool()).unwrap_or(false);

    let next_run_at_ms = state.get("nextRunAtMs").and_then(|v| v.as_i64());
    let last_run_at_ms = state
        .get("lastRunAtMs")
        .or_else(|| latest_run.get("runAtMs"))
        .and_then(|v| v.as_i64());
    let health_status = derive_cron_health(enabled, state);

    let summary_preview = latest_run
        .get("summary")
        .and_then(|v| v.as_str())
        .map(|s| {
            if s.chars().count() > 240 {
                truncate_chars(s, 240)
            } else {
                s.to_string()
            }
        })
        .filter(|s| !s.is_empty());

    json!({
        "schedule_label": schedule_label(job.get("schedule").unwrap_or(&Value::Null)),
        "next_run_at_ms": next_run_at_ms,
        "next_run_at": format_ts_ms(next_run_at_ms),
        "next_run_in": humanize_delta_ms(next_run_at_ms),
        "last_run_at_ms": last_run_at_ms,
        "last_run_at": format_ts_ms(last_run_at_ms),
        "last_run_ago": humanize_delta_ms(last_run_at_ms),
        "status_label": state.get("lastStatus")
            .or_else(|| state.get("lastRunStatus"))
            .and_then(|v| v.as_str())
            .unwrap_or("unknown"),
        "delivery_status_label": state.get("lastDeliveryStatus")
            .or_else(|| latest_run.get("deliveryStatus")),
        "health_status": health_status,
        "has_error": health_status == "warning" || health_status == "failing",
        "summary_preview": summary_preview,
    })
}

// ---------------------------------------------------------------------------
// Public interface
// ---------------------------------------------------------------------------

pub fn collect_instance_info(instance: &InstanceConfig) -> Value {
    let home = expand_home(&instance.openclaw_home);
    let config_file = home.join("openclaw.json");
    let cron_jobs_file = home.join("cron").join("jobs.json");

    if !config_file.exists() {
        return json!({
            "name": instance.name,
            "port": instance.port,
            "profile": instance.profile,
            "config_found": false,
        });
    }

    let config_mtime = file_mtime_millis(&config_file);

    let data: Value = match std::fs::read_to_string(&config_file)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
    {
        Some(v) => v,
        None => {
            return json!({
                "name": instance.name,
                "port": instance.port,
                "profile": instance.profile,
                "config_found": false,
                "error": "Failed to parse config",
            });
        }
    };

    let agents_section = data.get("agents").cloned().unwrap_or_else(|| json!({}));
    let default_workspace = agents_section
        .get("defaults")
        .and_then(|d| d.get("workspace"))
        .and_then(|v| v.as_str());
    let default_model = agents_section
        .get("defaults")
        .and_then(|d| d.get("model"))
        .and_then(|m| m.get("primary"))
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    let agents_list = agents_section
        .get("list")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    // Build soul mtime signatures for cache
    let cron_mtime = file_mtime_millis(&cron_jobs_file);
    let soul_mtimes: Vec<(String, Option<u64>)> = agents_list
        .iter()
        .filter_map(|a| a.as_object())
        .map(|a| {
            let id = a.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let ws = a
                .get("workspace")
                .and_then(|v| v.as_str())
                .or(default_workspace);
            let soul_path = resolve_soul_file(ws);
            let mtime = soul_path.as_ref().and_then(|p| file_mtime_millis(p));
            (id, mtime)
        })
        .collect();

    let cache_sig: CacheSignature = (config_mtime, cron_mtime, soul_mtimes);

    // Check cache
    {
        let guard = get_cache().lock().unwrap();
        if let Some(ref cache_map) = *guard {
            if let Some((ref sig, ref val)) = cache_map.get(&instance.name) {
                if *sig == cache_sig {
                    return val.clone();
                }
            }
        }
    }

    // Build agents
    let mut agents = Vec::new();
    for agent_val in &agents_list {
        let agent = match agent_val.as_object() {
            Some(o) => o,
            None => continue,
        };
        let ws = agent
            .get("workspace")
            .and_then(|v| v.as_str())
            .or(default_workspace);
        let soul_file = resolve_soul_file(ws);
        let model = agent
            .get("model")
            .and_then(|m| m.get("primary"))
            .and_then(|v| v.as_str())
            .or_else(|| {
                agent
                    .get("params")
                    .and_then(|p| p.get("model"))
                    .and_then(|m| m.get("primary"))
                    .and_then(|v| v.as_str())
            })
            .unwrap_or(default_model);

        let id = agent.get("id").and_then(|v| v.as_str()).unwrap_or("unknown");

        agents.push(json!({
            "id": id,
            "name": agent.get("name").and_then(|v| v.as_str()).unwrap_or(id),
            "is_default": agent.get("default").and_then(|v| v.as_bool()).unwrap_or(false),
            "workspace": ws,
            "model": model,
            "channels": [],
            "bindings": [],
            "soul": read_soul_file(soul_file.as_deref()),
            "crons": [],
            "cron_count": 0,
            "enabled_cron_count": 0,
        }));
    }

    // Build channels
    let mut channels = Vec::new();
    if let Some(channels_section) = data.get("channels").and_then(|v| v.as_object()) {
        for (ch_name, ch_conf) in channels_section {
            if let Some(conf) = ch_conf.as_object() {
                let accounts = conf.get("accounts").and_then(|v| v.as_object());
                let tools = conf.get("tools").and_then(|v| v.as_object());
                let account_count = accounts.map(|a| a.len()).unwrap_or(0);
                let tool_count = tools.map(|t| t.len()).unwrap_or(0);
                channels.push(json!({
                    "name": ch_name,
                    "enabled": conf.get("enabled").and_then(|v| v.as_bool()).unwrap_or(true),
                    "account_count": account_count,
                    "tool_count": tool_count,
                    "has_accounts": account_count > 0,
                    "has_tools": tool_count > 0,
                    "agent_ids": [],
                    "binding_count": 0,
                }));
            }
        }
    }

    // Process bindings
    let agent_map: HashMap<String, usize> = agents
        .iter()
        .enumerate()
        .filter_map(|(i, a)| a.get("id").and_then(|v| v.as_str()).map(|id| (id.to_string(), i)))
        .collect();
    let channel_map: HashMap<String, usize> = channels
        .iter()
        .enumerate()
        .filter_map(|(i, c)| {
            c.get("name")
                .and_then(|v| v.as_str())
                .map(|n| (n.to_string(), i))
        })
        .collect();

    if let Some(bindings) = data.get("bindings").and_then(|v| v.as_array()) {
        for binding in bindings {
            let binding = match binding.as_object() {
                Some(o) => o,
                None => continue,
            };
            let agent_id = match binding.get("agentId").and_then(|v| v.as_str()) {
                Some(id) => id,
                None => continue,
            };
            let match_obj = match binding.get("match").and_then(|v| v.as_object()) {
                Some(m) => m,
                None => continue,
            };
            let channel_name = match match_obj.get("channel").and_then(|v| v.as_str()) {
                Some(c) => c,
                None => continue,
            };

            let binding_info = json!({
                "channel": channel_name,
                "account_id": match_obj.get("accountId"),
            });

            if let Some(&idx) = agent_map.get(agent_id) {
                if let Some(agent) = agents.get_mut(idx) {
                    if let Some(bindings_arr) = agent.get_mut("bindings").and_then(|v| v.as_array_mut()) {
                        bindings_arr.push(binding_info);
                    }
                    if let Some(channels_arr) = agent.get_mut("channels").and_then(|v| v.as_array_mut()) {
                        let ch_val = json!(channel_name);
                        if !channels_arr.contains(&ch_val) {
                            channels_arr.push(ch_val);
                        }
                    }
                }
            }

            if let Some(&idx) = channel_map.get(channel_name) {
                if let Some(channel) = channels.get_mut(idx) {
                    if let Some(count) = channel.get_mut("binding_count") {
                        *count = json!(count.as_i64().unwrap_or(0) + 1);
                    }
                    if let Some(agent_ids) = channel.get_mut("agent_ids").and_then(|v| v.as_array_mut()) {
                        let aid_val = json!(agent_id);
                        if !agent_ids.contains(&aid_val) {
                            agent_ids.push(aid_val);
                        }
                    }
                }
            }
        }
    }

    // Crons
    let (crons, crons_by_agent) = read_cron_jobs(&home);
    for agent in &mut agents {
        let aid = agent.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let agent_crons = crons_by_agent.get(aid).cloned().unwrap_or_default();
        let enabled_count = agent_crons
            .iter()
            .filter(|c| c.get("enabled").and_then(|v| v.as_bool()).unwrap_or(false))
            .count();
        if let Some(obj) = agent.as_object_mut() {
            obj.insert("crons".into(), json!(agent_crons));
            obj.insert("cron_count".into(), json!(agent_crons.len()));
            obj.insert("enabled_cron_count".into(), json!(enabled_count));
        }
    }

    let enabled_channel_names: Vec<String> = channels
        .iter()
        .filter(|c| c.get("enabled").and_then(|v| v.as_bool()).unwrap_or(false))
        .filter_map(|c| c.get("name").and_then(|v| v.as_str()).map(|s| s.to_string()))
        .collect();
    let enabled_cron_count = crons
        .iter()
        .filter(|c| c.get("enabled").and_then(|v| v.as_bool()).unwrap_or(false))
        .count();

    let info = json!({
        "name": instance.name,
        "port": instance.port,
        "profile": instance.profile,
        "config_found": true,
        "default_model": default_model,
        "agents": agents,
        "agent_count": agents.len(),
        "channels": channels,
        "channel_count": channels.len(),
        "enabled_channel_names": enabled_channel_names,
        "enabled_channel_count": enabled_channel_names.len(),
        "crons": crons,
        "cron_count": crons.len(),
        "enabled_cron_count": enabled_cron_count,
    });

    // Update cache
    {
        let mut guard = get_cache().lock().unwrap();
        let cache_map = guard.get_or_insert_with(HashMap::new);
        cache_map.insert(instance.name.clone(), (cache_sig, info.clone()));
    }

    info
}

pub fn collect_all_info(config: &AppConfig) -> Vec<Value> {
    config
        .instances
        .iter()
        .filter(|i| i.enabled)
        .map(|i| collect_instance_info(i))
        .collect()
}
