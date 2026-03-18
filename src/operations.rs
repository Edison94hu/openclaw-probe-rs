use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::process::Command;
use tokio::sync::Mutex;

use crate::config::{expand_home, now_iso, InstanceConfig, SharedState};
use crate::database;

// ---------------------------------------------------------------------------
// In-memory task store
// ---------------------------------------------------------------------------

static TASKS: std::sync::LazyLock<Mutex<HashMap<String, Value>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));
static OPERATIONS_LOCK: std::sync::LazyLock<Mutex<()>> =
    std::sync::LazyLock::new(|| Mutex::new(()));

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

fn now_unix_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn normalize_version(v: &str) -> String {
    v.trim()
        .trim_start_matches('v')
        .trim_start_matches("openclaw")
        .trim()
        .to_string()
}

fn version_key(v: &str) -> Vec<u64> {
    normalize_version(v)
        .split(|c: char| !c.is_ascii_digit())
        .filter(|s| !s.is_empty())
        .filter_map(|s| s.parse().ok())
        .collect()
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

async fn get_npm_package_latest_version() -> (Option<String>, Option<String>) {
    let result = run_cmd(&["npm", "view", "openclaw", "version"], 20).await;
    let version = result
        .get("stdout")
        .and_then(|v| v.as_str())
        .map(normalize_version)
        .filter(|s| !s.is_empty());
    let error = result
        .get("stderr")
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .filter(|s| !s.is_empty());
    (version, error)
}

fn locate_openclaw_package_dir(npm_root: &str) -> Option<PathBuf> {
    let root = PathBuf::from(npm_root);

    let direct = root.join("openclaw");
    if direct.join("package.json").exists() {
        return Some(direct);
    }

    if let Ok(entries) = std::fs::read_dir(&root) {
        for entry in entries.flatten() {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with(".openclaw-") && path.join("package.json").exists() {
                return Some(path);
            }
        }
    }

    None
}

fn read_json_file(path: &Path) -> Option<Value> {
    let content = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&content).ok()
}

// ---------------------------------------------------------------------------
// OpenClaw install info
// ---------------------------------------------------------------------------

pub async fn get_openclaw_install_info() -> Value {
    let version = run_cmd(&["openclaw", "--version"], 20).await;
    let which_openclaw = run_cmd(&["which", "openclaw"], 20).await;
    let npm_root = run_cmd(&["npm", "root", "-g"], 20).await;
    let npm_root_path = npm_root
        .get("stdout")
        .and_then(|v| v.as_str())
        .map(str::to_string);

    let mut version_full = version
        .get("stdout")
        .or_else(|| version.get("stderr"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let mut install_strategy = "unknown";
    let mut npm_package_dir: Option<String> = None;
    let mut openclaw_path = which_openclaw
        .get("stdout")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let mut openclaw_version: Option<String> = if !version_full.is_empty() {
        let parts: Vec<&str> = version_full.split_whitespace().collect();
        if parts.len() >= 2 {
            Some(normalize_version(parts[1]))
        } else {
            None
        }
    } else {
        None
    };

    if let Some(root) = npm_root_path.as_deref() {
        if let Some(package_dir) = locate_openclaw_package_dir(root) {
            install_strategy = "npm-global";
            npm_package_dir = Some(package_dir.to_string_lossy().to_string());

            if openclaw_path.is_none() {
                let entry = package_dir.join("openclaw.mjs");
                if entry.exists() {
                    openclaw_path = Some(entry.to_string_lossy().to_string());
                }
            }

            if openclaw_version.is_none() || version_full.is_empty() {
                let build_info = read_json_file(&package_dir.join("dist").join("build-info.json"));
                let package_info = read_json_file(&package_dir.join("package.json"));
                let detected_version = build_info
                    .as_ref()
                    .and_then(|v| v.get("version"))
                    .and_then(|v| v.as_str())
                    .map(normalize_version)
                    .or_else(|| {
                        package_info
                            .as_ref()
                            .and_then(|v| v.get("version"))
                            .and_then(|v| v.as_str())
                            .map(normalize_version)
                    });
                if openclaw_version.is_none() {
                    openclaw_version = detected_version.clone();
                }
                if version_full.is_empty() {
                    version_full = match (
                        detected_version,
                        build_info
                            .as_ref()
                            .and_then(|v| v.get("commit"))
                            .and_then(|v| v.as_str()),
                    ) {
                        (Some(v), Some(commit)) if !commit.is_empty() => {
                            format!("OpenClaw {} ({})", v, &commit[..7.min(commit.len())])
                        }
                        (Some(v), _) => format!("OpenClaw {}", v),
                        _ => String::new(),
                    };
                }
            }
        }
    }

    let hostname = hostname::get()
        .map(|h| h.to_string_lossy().into_owned())
        .unwrap_or_default();

    json!({
        "hostname": hostname,
        "openclaw_version": openclaw_version,
        "openclaw_version_full": if version_full.is_empty() { Value::Null } else { json!(version_full) },
        "openclaw_path": openclaw_path,
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
    let install_strategy = install_info
        .get("install_strategy")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();

    let api_url = "https://api.github.com/repos/openclaw/openclaw/releases/latest";
    let release_url = "https://github.com/openclaw/openclaw/releases";
    let (package_latest_version, package_error) = if install_strategy == "npm-global" {
        get_npm_package_latest_version().await
    } else {
        (None, None)
    };

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
        "release_update_available": null,
        "package_name": if install_strategy == "npm-global" { json!("openclaw") } else { Value::Null },
        "package_latest_version": package_latest_version,
        "package_error": package_error,
        "package_update_available": null,
        "install_target_version": null,
        "install_target_source": null,
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
                    .map(normalize_version);

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
                }
            }
        }
        Err(e) => {
            if let Some(obj) = result.as_object_mut() {
                obj.insert("error".into(), json!(e.to_string()));
            }
        }
    }

    if let Some(obj) = result.as_object_mut() {
        let release_version = obj
            .get("latest_version")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        let release_update_available = match (&current_version, &release_version) {
            (Some(cv), Some(rv)) => Some(version_key(rv) > version_key(cv)),
            _ => None,
        };
        let package_update_available = match (&current_version, &package_latest_version) {
            (Some(cv), Some(pv)) => Some(version_key(pv) > version_key(cv)),
            _ => None,
        };

        obj.insert("release_update_available".into(), json!(release_update_available));
        obj.insert("package_update_available".into(), json!(package_update_available));

        let (install_target_version, install_target_source, update_available) =
            if install_strategy == "npm-global" {
                (
                    package_latest_version.clone(),
                    Some("npm-dist-tag".to_string()),
                    package_update_available,
                )
            } else {
                (
                    release_version.clone(),
                    Some("github-release".to_string()),
                    release_update_available,
                )
            };

        obj.insert("install_target_version".into(), json!(install_target_version));
        obj.insert("install_target_source".into(), json!(install_target_source));
        obj.insert("update_available".into(), json!(update_available));
    }

    result
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

fn enabled_instances(config: &crate::config::AppConfig) -> Vec<InstanceConfig> {
    config.instances.iter().filter(|inst| inst.enabled).cloned().collect()
}

async fn wait_for_instances_health(
    config: &crate::config::AppConfig,
    expected_up: bool,
    timeout_seconds: u64,
) -> bool {
    let instances = enabled_instances(config);
    if instances.is_empty() {
        return true;
    }

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_seconds);
    let client = reqwest::Client::new();
    while std::time::Instant::now() < deadline {
        let mut results = Vec::with_capacity(instances.len());
        for inst in &instances {
            let url = format!("http://127.0.0.1:{}/health", inst.port);
            let ok = match client
                .get(&url)
                .timeout(std::time::Duration::from_secs(config.probe.timeout_seconds))
                .send()
                .await
            {
                Ok(resp) => (200..300).contains(&resp.status().as_u16()),
                Err(_) => false,
            };
            results.push(ok);
        }

        if expected_up && results.iter().all(|ok| *ok) {
            return true;
        }
        if !expected_up && results.iter().all(|ok| !*ok) {
            return true;
        }

        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }

    false
}

async fn run_instance_action(
    config: &crate::config::AppConfig,
    action: &str,
    timeout_secs: u64,
) -> Vec<Value> {
    let mut results = Vec::new();
    for inst in enabled_instances(config) {
        let mut cmd = vec![
            "openclaw".to_string(),
            "gateway".to_string(),
            action.to_string(),
        ];
        if let Some(profile) = inst.profile {
            cmd.push("--profile".to_string());
            cmd.push(profile.to_string());
        }

        let cmd_refs: Vec<&str> = cmd.iter().map(String::as_str).collect();
        let result = run_cmd(&cmd_refs, timeout_secs).await;
        results.push(json!({
            "instance_name": inst.name,
            "command": cmd,
            "success": result.get("success"),
            "returncode": result.get("returncode"),
            "stdout": result.get("stdout"),
            "stderr": result.get("stderr"),
        }));
    }
    results
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
    launch_task(task_id, move || async move { perform_backup(&state, label).await });

    task
}

fn restore_backup_sync(metadata: &Value) -> anyhow::Result<Value> {
    let archive_path = PathBuf::from(
        metadata
            .get("archive_path")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing archive_path"))?,
    );
    if !archive_path.exists() {
        anyhow::bail!("Backup archive not found: {}", archive_path.display());
    }

    let temp_root = std::env::temp_dir().join(format!("openclaw-restore-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&temp_root)?;
    let mut moved_paths: Vec<(PathBuf, PathBuf)> = Vec::new();

    let result = (|| -> anyhow::Result<Value> {
        let file = std::fs::File::open(&archive_path)?;
        let dec = flate2::read::GzDecoder::new(file);
        let mut archive = tar::Archive::new(dec);
        archive.unpack(&temp_root)?;

        let homes = metadata
            .get("homes")
            .and_then(|v| v.as_array())
            .ok_or_else(|| anyhow::anyhow!("Backup homes metadata missing"))?;

        for home in homes {
            let source_path = PathBuf::from(expand_home(
                home.get("source_path")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| anyhow::anyhow!("Backup source_path missing"))?,
            ));
            let extracted_path = temp_root.join(
                home.get("archive_path")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| anyhow::anyhow!("Backup archive_path missing"))?,
            );

            if !extracted_path.exists() {
                anyhow::bail!("Backup content missing: {}", extracted_path.display());
            }

            if source_path.exists() {
                let moved_path = source_path.parent().unwrap_or(Path::new("/")).join(format!(
                    "{}.pre-restore-{}",
                    source_path
                        .file_name()
                        .and_then(|v| v.to_str())
                        .unwrap_or("openclaw"),
                    now_unix_seconds()
                ));
                std::fs::rename(&source_path, &moved_path)?;
                moved_paths.push((source_path.clone(), moved_path));
            }

            std::fs::rename(&extracted_path, &source_path)?;
        }

        Ok(json!({
            "moved_paths": moved_paths.iter().map(|(source_path, moved_path)| {
                json!({
                    "source_path": source_path.to_string_lossy(),
                    "moved_path": moved_path.to_string_lossy(),
                })
            }).collect::<Vec<_>>()
        }))
    })();

    if result.is_err() {
        for (source_path, moved_path) in moved_paths.iter().rev() {
            if source_path.exists() {
                if source_path.is_dir() {
                    let _ = std::fs::remove_dir_all(source_path);
                } else {
                    let _ = std::fs::remove_file(source_path);
                }
            }
            if moved_path.exists() {
                let _ = std::fs::rename(moved_path, source_path);
            }
        }
    }

    let _ = std::fs::remove_dir_all(&temp_root);
    result
}

async fn with_maintenance<F, Fut>(state: &SharedState, worker: F) -> anyhow::Result<Value>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<Value>>,
{
    let original = database::get_maintenance_mode(&state.db).await.unwrap_or(false);
    if !original {
        let _ = database::set_maintenance_mode(&state.db, true).await;
        let _ = database::insert_event(
            &state.db,
            "maintenance_entered",
            "Agent entered maintenance mode",
            None,
            None,
        )
        .await;
    }

    let result = worker().await;

    if !original {
        let _ = database::set_maintenance_mode(&state.db, false).await;
        let _ = database::insert_event(
            &state.db,
            "maintenance_exited",
            "Agent exited maintenance mode",
            None,
            None,
        )
        .await;
    }

    result
}

async fn perform_restore(
    state: &SharedState,
    backup_id: &str,
    start_after_restore: bool,
) -> anyhow::Result<Value> {
    let metadata = get_backup(state, backup_id)
        .await
        .ok_or_else(|| anyhow::anyhow!("Backup '{}' not found", backup_id))?;
    let config = state.config.get().await;

    with_maintenance(state, || async move {
        let stop_results = run_instance_action(&config, "stop", 120).await;
        let _ = wait_for_instances_health(&config, false, 30).await;
        let restore = tokio::task::spawn_blocking(move || restore_backup_sync(&metadata))
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))??;

        let mut start_results = Vec::new();
        let mut health_ok = true;
        if start_after_restore {
            start_results = run_instance_action(&config, "start", 120).await;
            health_ok = wait_for_instances_health(&config, true, 90).await;
        }

        let _ = database::insert_event(
            &state.db,
            if health_ok { "backup_restored" } else { "backup_restore_failed" },
            &format!("已恢复备份 {}", backup_id),
            None,
            None,
        )
        .await;

        Ok(json!({
            "backup_id": backup_id,
            "stop_results": stop_results,
            "restore": restore,
            "start_results": start_results,
            "health_ok": health_ok,
        }))
    })
    .await
}

async fn perform_upgrade(
    state: &SharedState,
    target_version: &str,
    create_backup: bool,
    rollback_on_failure: bool,
) -> anyhow::Result<Value> {
    let install_info = get_openclaw_install_info().await;
    let install_strategy = install_info
        .get("install_strategy")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    if install_strategy != "npm-global" {
        anyhow::bail!(
            "Unsupported install strategy: {}. Only npm-global upgrades are supported right now.",
            install_strategy
        );
    }

    let previous_version = install_info
        .get("openclaw_version")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let config = state.config.get().await;
    let package = if target_version.is_empty() || target_version == "latest" {
        "openclaw@latest".to_string()
    } else {
        format!("openclaw@{}", target_version)
    };

    with_maintenance(state, || async move {
        let mut backup_metadata = Value::Null;
        if create_backup {
            backup_metadata = perform_backup(state, Some(format!(
                "pre-upgrade-{}",
                previous_version.clone().unwrap_or_else(|| "unknown".to_string())
            )))
            .await?;
        }

        let install_cmd = ["npm", "install", "-g", package.as_str()];
        let install_result = run_cmd(&install_cmd, 1800).await;
        if !install_result
            .get("success")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            anyhow::bail!(
                "{}",
                install_result
                    .get("stderr")
                    .and_then(|v| v.as_str())
                    .or_else(|| install_result.get("stdout").and_then(|v| v.as_str()))
                    .unwrap_or("Upgrade command failed")
            );
        }

        let restart_results = run_instance_action(&config, "restart", 180).await;
        let health_ok = wait_for_instances_health(&config, true, 120).await;
        let current_info = get_openclaw_install_info().await;

        let mut rollback_result = Value::Null;
        if !health_ok && rollback_on_failure {
            if let Some(prev) = previous_version.as_deref() {
                let rollback_install_cmd = ["npm", "install", "-g", &format!("openclaw@{}", prev)];
                let rollback_install = run_cmd(&rollback_install_cmd, 1800).await;
                let rollback_restart = run_instance_action(&config, "restart", 180).await;
                let rollback_health = wait_for_instances_health(&config, true, 120).await;
                rollback_result = json!({
                    "install": rollback_install,
                    "restart_results": rollback_restart,
                    "health_ok": rollback_health,
                    "backup_id": backup_metadata.get("backup_id"),
                });
            }
        }

        let _ = database::insert_event(
            &state.db,
            if health_ok { "upgrade_success" } else { "upgrade_failed" },
            &format!(
                "OpenClaw upgrade target={} current={}",
                if target_version.is_empty() { "latest" } else { target_version },
                current_info
                    .get("openclaw_version")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
            ),
            None,
            None,
        )
        .await;

        Ok(json!({
            "target_version": if target_version.is_empty() { "latest" } else { target_version },
            "previous_version": previous_version,
            "current_version": current_info.get("openclaw_version"),
            "backup": backup_metadata,
            "install_result": install_result,
            "restart_results": restart_results,
            "health_ok": health_ok,
            "rollback_result": rollback_result,
        }))
    })
    .await
}

async fn perform_backup(
    state: &SharedState,
    label: Option<String>,
) -> anyhow::Result<Value> {
    let config = state.config.get().await;
    let homes = unique_homes(&config);
    if homes.is_empty() {
        anyhow::bail!("No enabled OpenClaw homes");
    }

    for (path, _) in &homes {
        if !path.exists() {
            anyhow::bail!("Backup source not found: {}", path.display());
        }
    }

    let root = backup_root(state, &config);
    std::fs::create_dir_all(&root)?;

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

    tokio::task::spawn_blocking({
        let homes = homes.clone();
        let archive_path = archive_path.clone();
        let metadata_path = metadata_path.clone();
        let metadata_for_write = metadata.clone();
        move || -> anyhow::Result<()> {
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
            std::fs::write(&metadata_path, serde_json::to_string_pretty(&metadata_for_write)?)?;
            Ok(())
        }
    })
    .await
    .map_err(|e| anyhow::anyhow!(e.to_string()))??;

    let _ = database::insert_event(
        &state.db,
        "backup_created",
        &format!("已创建备份 {}", backup_id),
        None,
        None,
    )
    .await;

    Ok(metadata)
}

fn launch_task<F, Fut>(
    task_id: String,
    worker: F,
) where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = anyhow::Result<Value>> + Send + 'static,
{
    tokio::spawn(async move {
        let _guard = OPERATIONS_LOCK.lock().await;
        update_task(
            &task_id,
            &[("status", json!("running")), ("started_at", json!(now_iso()))],
        )
        .await;

        match worker().await {
            Ok(result) => {
                update_task(
                    &task_id,
                    &[
                        ("status", json!("success")),
                        ("result", result),
                        ("finished_at", json!(now_iso())),
                    ],
                )
                .await;
            }
            Err(err) => {
                update_task(
                    &task_id,
                    &[
                        ("status", json!("failed")),
                        ("error", json!(err.to_string())),
                        ("finished_at", json!(now_iso())),
                    ],
                )
                .await;
            }
        }
    });
}

pub async fn create_restore_task(
    state: Arc<crate::config::AppState>,
    backup_id: String,
    start_after_restore: bool,
) -> Value {
    let task = create_task(
        "restore",
        json!({
            "backup_id": backup_id,
            "start_after_restore": start_after_restore,
        }),
    )
    .await;
    let task_id = task
        .get("task_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    launch_task(task_id, move || async move {
        perform_restore(&state, &backup_id, start_after_restore).await
    });
    task
}

pub async fn create_upgrade_task(
    state: Arc<crate::config::AppState>,
    target_version: String,
    create_backup: bool,
    rollback_on_failure: bool,
) -> Value {
    let task = create_task(
        "upgrade",
        json!({
            "target_version": target_version,
            "create_backup": create_backup,
            "rollback_on_failure": rollback_on_failure,
        }),
    )
    .await;
    let task_id = task
        .get("task_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    launch_task(task_id, move || async move {
        perform_upgrade(&state, &target_version, create_backup, rollback_on_failure).await
    });
    task
}

pub async fn create_upgrade_if_needed_task(
    state: Arc<crate::config::AppState>,
    create_backup: bool,
    rollback_on_failure: bool,
    force_refresh_release: bool,
) -> Value {
    let task = create_task(
        "upgrade-if-needed",
        json!({
            "create_backup": create_backup,
            "rollback_on_failure": rollback_on_failure,
            "force_refresh_release": force_refresh_release,
        }),
    )
    .await;
    let task_id = task
        .get("task_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    launch_task(task_id, move || async move {
        let release = get_openclaw_latest_release(force_refresh_release).await;
        if let Some(err) = release.get("error").and_then(|v| v.as_str()) {
            anyhow::bail!("Failed to check latest release: {}", err);
        }

        let latest_version = release
            .get("latest_version")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Latest release version not found"))?;
        let install_target_version = release
            .get("install_target_version")
            .and_then(|v| v.as_str());
        let current_version = release.get("current_version").and_then(|v| v.as_str());
        let update_available = release.get("update_available").and_then(|v| v.as_bool());

        if update_available == Some(false)
            || install_target_version == current_version
            || current_version == Some(latest_version)
        {
            let _ = database::insert_event(
                &state.db,
                "upgrade_skipped",
                &format!(
                    "OpenClaw 已是最新可安装版本 {}",
                    current_version
                        .or(install_target_version)
                        .unwrap_or(latest_version)
                ),
                None,
                None,
            )
            .await;
            return Ok(json!({
                "skipped": true,
                "reason": "already_up_to_date",
                "current_version": current_version,
                "latest_version": latest_version,
                "install_target_version": install_target_version,
                "release": release,
            }));
        }

        let install_target_version = match install_target_version {
            Some(v) => v,
            None => {
                let _ = database::insert_event(
                    &state.db,
                    "upgrade_skipped",
                    &format!("发现 GitHub 新版本 {}，但 npm 暂无可安装版本", latest_version),
                    None,
                    None,
                )
                .await;
                return Ok(json!({
                    "skipped": true,
                    "reason": "no_installable_update",
                    "current_version": current_version,
                    "latest_version": latest_version,
                    "install_target_version": null,
                    "release": release,
                }));
            }
        };

        let mut result =
            perform_upgrade(&state, install_target_version, create_backup, rollback_on_failure).await?;
        if let Some(obj) = result.as_object_mut() {
            obj.insert("release".into(), release);
        }
        Ok(result)
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
