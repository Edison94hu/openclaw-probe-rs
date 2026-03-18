use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::process::Command;
use tokio::sync::Mutex;

use crate::config::{expand_home, now_iso, SharedState};
use crate::database;

// ---------------------------------------------------------------------------
// In-memory task store
// ---------------------------------------------------------------------------

static TASKS: std::sync::LazyLock<Mutex<HashMap<String, Value>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));

async fn create_task(kind: &str, payload: Value) -> Value {
    let task_id = uuid::Uuid::new_v4().to_string()[..12].to_string();
    let now = now_iso();
    let task = json!({
        "task_id": task_id,
        "kind": kind,
        "status": "queued",
        "payload": payload,
        "result": null,
        "error": null,
        "created_at": now,
        "started_at": null,
        "finished_at": null,
        "updated_at": now,
    });

    let mut tasks = TASKS.lock().await;
    tasks.insert(task_id.clone(), task.clone());

    // Prune to 100
    if tasks.len() > 100 {
        let mut entries: Vec<(String, String)> = tasks
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    v.get("created_at")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                )
            })
            .collect();
        entries.sort_by(|a, b| b.1.cmp(&a.1));
        let to_remove: Vec<String> = entries[100..].iter().map(|(k, _)| k.clone()).collect();
        for k in to_remove {
            tasks.remove(&k);
        }
    }

    task
}

async fn update_task(task_id: &str, updates: &[(&str, Value)]) {
    let mut tasks = TASKS.lock().await;
    if let Some(task) = tasks.get_mut(task_id) {
        if let Some(obj) = task.as_object_mut() {
            for (key, val) in updates {
                obj.insert(key.to_string(), val.clone());
            }
            obj.insert("updated_at".to_string(), json!(now_iso()));
        }
    }
}

pub async fn get_task(task_id: &str) -> Option<Value> {
    let tasks = TASKS.lock().await;
    tasks.get(task_id).cloned()
}

pub async fn list_tasks(limit: usize) -> Vec<Value> {
    let tasks = TASKS.lock().await;
    let mut entries: Vec<&Value> = tasks.values().collect();
    entries.sort_by(|a, b| {
        let a_ts = a.get("created_at").and_then(|v| v.as_str()).unwrap_or("");
        let b_ts = b.get("created_at").and_then(|v| v.as_str()).unwrap_or("");
        b_ts.cmp(a_ts)
    });
    entries.into_iter().take(limit).cloned().collect()
}

// ---------------------------------------------------------------------------
// Run shell command helper
// ---------------------------------------------------------------------------

async fn run_cmd(cmd: &[&str], timeout_secs: u64) -> Value {
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(timeout_secs),
        Command::new(cmd[0]).args(&cmd[1..]).output(),
    )
    .await;

    match result {
        Ok(Ok(output)) => {
            let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            json!({
                "success": output.status.success(),
                "returncode": output.status.code(),
                "stdout": if stdout.is_empty() { Value::Null } else { json!(stdout) },
                "stderr": if stderr.is_empty() { Value::Null } else { json!(stderr) },
            })
        }
        Ok(Err(e)) => json!({
            "success": false,
            "returncode": null,
            "stdout": null,
            "stderr": e.to_string(),
        }),
        Err(_) => json!({
            "success": false,
            "returncode": null,
            "stdout": null,
            "stderr": "Command timed out",
        }),
    }
}

// ---------------------------------------------------------------------------
// OpenClaw install info
// ---------------------------------------------------------------------------

pub async fn get_openclaw_install_info() -> Value {
    let version = run_cmd(&["openclaw", "--version"], 20).await;
    let which_openclaw = run_cmd(&["which", "openclaw"], 20).await;
    let npm_root = run_cmd(&["npm", "root", "-g"], 20).await;

    let version_full = version
        .get("stdout")
        .or_else(|| version.get("stderr"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let openclaw_version = if !version_full.is_empty() {
        let parts: Vec<&str> = version_full.split_whitespace().collect();
        if parts.len() >= 2 {
            Some(parts[1].to_string())
        } else {
            None
        }
    } else {
        None
    };

    let mut install_strategy = "unknown";
    let mut npm_package_dir: Option<String> = None;
    if let Some(root) = npm_root.get("stdout").and_then(|v| v.as_str()) {
        let candidate = PathBuf::from(root).join("openclaw");
        if candidate.exists() {
            install_strategy = "npm-global";
            npm_package_dir = Some(candidate.to_string_lossy().to_string());
        }
    }

    let hostname = hostname::get()
        .map(|h| h.to_string_lossy().into_owned())
        .unwrap_or_default();

    json!({
        "hostname": hostname,
        "openclaw_version": openclaw_version,
        "openclaw_version_full": if version_full.is_empty() { Value::Null } else { json!(version_full) },
        "openclaw_path": which_openclaw.get("stdout"),
        "install_strategy": install_strategy,
        "npm_package_dir": npm_package_dir,
    })
}

// ---------------------------------------------------------------------------
// Latest release
// ---------------------------------------------------------------------------

pub async fn get_openclaw_latest_release(_force_refresh: bool) -> Value {
    let install_info = get_openclaw_install_info().await;
    let current_version = install_info
        .get("openclaw_version")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let api_url = "https://api.github.com/repos/openclaw/openclaw/releases/latest";
    let release_url = "https://github.com/openclaw/openclaw/releases";

    let mut result = json!({
        "source": "github-releases",
        "repo": "openclaw/openclaw",
        "release_url": release_url,
        "api_url": api_url,
        "current_version": current_version,
        "latest_version": null,
        "latest_tag": null,
        "latest_name": null,
        "published_at": null,
        "html_url": release_url,
        "prerelease": null,
        "draft": null,
        "update_available": null,
        "fetched_at": now_iso(),
        "error": null,
    });

    let client = reqwest::Client::new();
    match client
        .get(api_url)
        .header("Accept", "application/vnd.github+json")
        .header("User-Agent", "openclaw-host-agent")
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await
    {
        Ok(resp) => {
            if let Ok(payload) = resp.json::<Value>().await {
                let latest_tag = payload.get("tag_name").and_then(|v| v.as_str());
                let latest_version = latest_tag
                    .or_else(|| payload.get("name").and_then(|v| v.as_str()))
                    .map(|s| s.trim().trim_start_matches('v').to_string());

                if let Some(obj) = result.as_object_mut() {
                    obj.insert("latest_version".into(), json!(latest_version));
                    obj.insert("latest_tag".into(), json!(latest_tag));
                    obj.insert("latest_name".into(), json!(payload.get("name")));
                    obj.insert("published_at".into(), json!(payload.get("published_at")));
                    obj.insert(
                        "html_url".into(),
                        json!(payload
                            .get("html_url")
                            .and_then(|v| v.as_str())
                            .unwrap_or(release_url)),
                    );
                    obj.insert("prerelease".into(), json!(payload.get("prerelease")));
                    obj.insert("draft".into(), json!(payload.get("draft")));

                    if let (Some(cv), Some(lv)) = (&current_version, &latest_version) {
                        let current_parts = parse_version(cv);
                        let latest_parts = parse_version(lv);
                        obj.insert(
                            "update_available".into(),
                            json!(latest_parts > current_parts),
                        );
                    }
                }
            }
        }
        Err(e) => {
            if let Some(obj) = result.as_object_mut() {
                obj.insert("error".into(), json!(e.to_string()));
            }
        }
    }

    result
}

fn parse_version(v: &str) -> Vec<u64> {
    v.trim()
        .trim_start_matches('v')
        .split(|c: char| !c.is_ascii_digit())
        .filter(|s| !s.is_empty())
        .filter_map(|s| s.parse().ok())
        .collect()
}

// ---------------------------------------------------------------------------
// Backup (simplified)
// ---------------------------------------------------------------------------

fn backup_root(_state: &SharedState, config: &crate::config::AppConfig) -> PathBuf {
    expand_home(&config.operations.backup_dir)
}

fn unique_homes(config: &crate::config::AppConfig) -> Vec<(PathBuf, Vec<String>)> {
    let mut homes: HashMap<String, (PathBuf, Vec<String>)> = HashMap::new();
    for inst in &config.instances {
        if !inst.enabled {
            continue;
        }
        let home = expand_home(&inst.openclaw_home);
        let key = home.to_string_lossy().to_string();
        homes
            .entry(key)
            .or_insert_with(|| (home, vec![]))
            .1
            .push(inst.name.clone());
    }
    homes.into_values().collect()
}

pub async fn list_backups(state: &SharedState) -> Vec<Value> {
    let config = state.config.get().await;
    let root = backup_root(state, &config);
    if !root.exists() {
        return vec![];
    }

    let mut items = Vec::new();
    if let Ok(entries) = std::fs::read_dir(&root) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("json") {
                if let Ok(content) = std::fs::read_to_string(&path) {
                    if let Ok(mut meta) = serde_json::from_str::<Value>(&content) {
                        let archive_path = meta
                            .get("archive_path")
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        let archive_exists = PathBuf::from(archive_path).exists();
                        let archive_size = if archive_exists {
                            std::fs::metadata(archive_path)
                                .map(|m| m.len())
                                .unwrap_or(0)
                        } else {
                            0
                        };
                        if let Some(obj) = meta.as_object_mut() {
                            obj.insert("archive_exists".into(), json!(archive_exists));
                            obj.insert("archive_size_bytes".into(), json!(archive_size));
                        }
                        items.push(meta);
                    }
                }
            }
        }
    }
    items.sort_by(|a, b| {
        let a_ts = a.get("created_at").and_then(|v| v.as_str()).unwrap_or("");
        let b_ts = b.get("created_at").and_then(|v| v.as_str()).unwrap_or("");
        b_ts.cmp(a_ts)
    });
    items
}

pub async fn get_backup(state: &SharedState, backup_id: &str) -> Option<Value> {
    let backups = list_backups(state).await;
    backups
        .into_iter()
        .find(|b| b.get("backup_id").and_then(|v| v.as_str()) == Some(backup_id))
}

pub async fn create_backup_task(state: Arc<crate::config::AppState>, label: Option<String>) -> Value {
    let task = create_task("backup", json!({ "label": label.as_deref().unwrap_or("") })).await;
    let task_id = task
        .get("task_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    tokio::spawn(async move {
        update_task(&task_id, &[("status", json!("running")), ("started_at", json!(now_iso()))]).await;

        let config = state.config.get().await;
        let homes = unique_homes(&config);
        if homes.is_empty() {
            update_task(&task_id, &[
                ("status", json!("failed")),
                ("error", json!("No enabled OpenClaw homes")),
                ("finished_at", json!(now_iso())),
            ]).await;
            return;
        }

        let root = backup_root(&state, &config);
        if let Err(e) = std::fs::create_dir_all(&root) {
            update_task(&task_id, &[
                ("status", json!("failed")),
                ("error", json!(e.to_string())),
                ("finished_at", json!(now_iso())),
            ]).await;
            return;
        }

        let backup_id = format!("backup-{}", chrono::Local::now().format("%Y%m%d-%H%M%S"));
        let archive_path = root.join(format!("{backup_id}.tar.gz"));
        let metadata_path = root.join(format!("{backup_id}.json"));

        let install_info = get_openclaw_install_info().await;

        let homes_meta: Vec<Value> = homes
            .iter()
            .enumerate()
            .map(|(idx, (path, instances))| {
                json!({
                    "source_path": path.to_string_lossy(),
                    "archive_path": format!("homes/{}/{}", idx, path.file_name().unwrap_or_default().to_string_lossy()),
                    "instances": instances,
                })
            })
            .collect();

        let metadata = json!({
            "backup_id": backup_id,
            "label": label.as_deref().unwrap_or(""),
            "created_at": now_iso(),
            "hostname": hostname::get().map(|h| h.to_string_lossy().into_owned()).unwrap_or_default(),
            "openclaw_version": install_info.get("openclaw_version"),
            "openclaw_version_full": install_info.get("openclaw_version_full"),
            "archive_path": archive_path.to_string_lossy(),
            "metadata_path": metadata_path.to_string_lossy(),
            "homes": homes_meta,
        });

        // Create tar.gz
        match (|| -> anyhow::Result<()> {
            let file = std::fs::File::create(&archive_path)?;
            let enc = flate2::write::GzEncoder::new(file, flate2::Compression::default());
            let mut tar_builder = tar::Builder::new(enc);
            for (path, _) in &homes {
                if path.exists() {
                    let arc_name = format!("homes/{}", path.file_name().unwrap_or_default().to_string_lossy());
                    tar_builder.append_dir_all(&arc_name, path)?;
                }
            }
            tar_builder.finish()?;
            Ok(())
        })() {
            Ok(_) => {
                let _ = std::fs::write(&metadata_path, serde_json::to_string_pretty(&metadata).unwrap_or_default());
                let _ = database::insert_event(&state.db, "backup_created", &format!("已创建备份 {backup_id}"), None, None).await;
                update_task(&task_id, &[
                    ("status", json!("success")),
                    ("result", metadata),
                    ("finished_at", json!(now_iso())),
                ]).await;
            }
            Err(e) => {
                update_task(&task_id, &[
                    ("status", json!("failed")),
                    ("error", json!(e.to_string())),
                    ("finished_at", json!(now_iso())),
                ]).await;
            }
        }
    });

    task
}

// ---------------------------------------------------------------------------
// Maintenance mode
// ---------------------------------------------------------------------------

pub async fn enter_maintenance(state: &SharedState, reason: Option<&str>) -> Value {
    let _ = database::set_maintenance_mode(&state.db, true).await;
    let _ = database::insert_event(
        &state.db,
        "maintenance_entered",
        reason.unwrap_or("Agent entered maintenance mode"),
        None,
        None,
    )
    .await;
    json!({
        "maintenance_mode": true,
        "reason": reason.unwrap_or(""),
    })
}

pub async fn exit_maintenance(state: &SharedState, reason: Option<&str>) -> Value {
    let _ = database::set_maintenance_mode(&state.db, false).await;
    let _ = database::insert_event(
        &state.db,
        "maintenance_exited",
        reason.unwrap_or("Agent exited maintenance mode"),
        None,
        None,
    )
    .await;
    json!({
        "maintenance_mode": false,
        "reason": reason.unwrap_or(""),
    })
}
