use serde_json::{json, Value};
use std::process::Command;
use std::sync::Mutex;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

static CACHE: Mutex<Option<(Instant, Value)>> = Mutex::new(None);
const CACHE_TTL_SECS: u64 = 30;

fn run_text(cmd: &[&str]) -> Option<String> {
    let output = Command::new(cmd[0])
        .args(&cmd[1..])
        .output()
        .ok()?;
    let text = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if text.is_empty() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        if stderr.is_empty() {
            None
        } else {
            Some(stderr)
        }
    } else {
        Some(text)
    }
}

fn parse_sysctl_int(key: &str) -> Option<i64> {
    run_text(&["sysctl", "-n", key])?
        .trim()
        .parse::<i64>()
        .ok()
}

fn parse_vm_stat() -> Value {
    let output = match run_text(&["vm_stat"]) {
        Some(o) => o,
        None => return json!({}),
    };

    let lines: Vec<&str> = output.lines().collect();
    let mut page_size: i64 = 4096;
    if let Some(first) = lines.first() {
        if let Some(cap) = first.find("page size of ") {
            let rest = &first[cap + 13..];
            if let Some(end) = rest.find(' ') {
                if let Ok(ps) = rest[..end].parse::<i64>() {
                    page_size = ps;
                }
            }
        }
    }

    let mut values = std::collections::HashMap::new();
    for line in &lines[1..] {
        if let Some(colon_pos) = line.find(':') {
            let key = line[..colon_pos].trim().trim_matches('"');
            let raw = line[colon_pos + 1..]
                .trim()
                .trim_end_matches('.')
                .replace(['.', ','], "");
            if let Ok(num) = raw.parse::<i64>() {
                values.insert(key.to_string(), num);
            }
        }
    }

    let to_bytes = |key: &str| -> i64 { values.get(key).copied().unwrap_or(0) * page_size };

    json!({
        "page_size": page_size,
        "free_bytes": to_bytes("Pages free"),
        "active_bytes": to_bytes("Pages active"),
        "inactive_bytes": to_bytes("Pages inactive"),
        "speculative_bytes": to_bytes("Pages speculative"),
        "wired_bytes": to_bytes("Pages wired down"),
        "compressed_bytes": to_bytes("Pages occupied by compressor"),
        "purgeable_bytes": to_bytes("Pages purgeable"),
        "file_backed_bytes": to_bytes("File-backed pages"),
        "anonymous_bytes": to_bytes("Anonymous pages"),
    })
}

fn parse_swap_usage() -> Value {
    let output = match run_text(&["sysctl", "vm.swapusage"]) {
        Some(o) => o,
        None => return json!({}),
    };

    fn extract(output: &str, label: &str) -> Option<i64> {
        let pat = format!("{label} = ");
        let start = output.find(&pat)? + pat.len();
        let rest = &output[start..];
        let end = rest.find(|c: char| !c.is_ascii_digit() && c != '.')?;
        let num: f64 = rest[..end].parse().ok()?;
        let unit = rest[end..end + 1].chars().next()?;
        let scale: f64 = match unit {
            'K' => 1024.0,
            'M' => 1024.0 * 1024.0,
            'G' => 1024.0 * 1024.0 * 1024.0,
            'T' => 1024.0 * 1024.0 * 1024.0 * 1024.0,
            _ => 1.0,
        };
        Some((num * scale) as i64)
    }

    json!({
        "total_bytes": extract(&output, "total"),
        "used_bytes": extract(&output, "used"),
        "free_bytes": extract(&output, "free"),
        "raw": output,
    })
}

fn parse_boot_time() -> Option<f64> {
    let output = run_text(&["sysctl", "-n", "kern.boottime"])?;
    let sec_pos = output.find("sec = ")?;
    let rest = &output[sec_pos + 6..];
    let end = rest.find(|c: char| !c.is_ascii_digit())?;
    rest[..end].parse::<f64>().ok()
}

fn process_rss_bytes(pid: u32) -> Option<i64> {
    let output = run_text(&["ps", "-p", &pid.to_string(), "-o", "rss="])?;
    let kb: i64 = output.trim().parse().ok()?;
    Some(kb * 1024)
}

fn disk_usage(path: &str) -> (u64, u64, u64) {
    match nix_statvfs(path) {
        Some((total, free)) => {
            let used = total.saturating_sub(free);
            (total, used, free)
        }
        None => (0, 0, 0),
    }
}

fn nix_statvfs(path: &str) -> Option<(u64, u64)> {
    // Use df as a portable fallback
    let output = Command::new("df")
        .arg("-k")
        .arg(path)
        .output()
        .ok()?;
    let text = String::from_utf8_lossy(&output.stdout);
    let line = text.lines().nth(1)?;
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() < 4 {
        return None;
    }
    let total_kb: u64 = parts[1].parse().ok()?;
    let _used_kb: u64 = parts[2].parse().ok()?;
    let avail_kb: u64 = parts[3].parse().ok()?;
    Some((total_kb * 1024, avail_kb * 1024))
}

fn load_avg() -> (f64, f64, f64) {
    match run_text(&["sysctl", "-n", "vm.loadavg"]) {
        Some(output) => {
            // Output: "{ 1.23 4.56 7.89 }"
            let nums: Vec<f64> = output
                .trim_matches(|c: char| c == '{' || c == '}' || c.is_whitespace())
                .split_whitespace()
                .filter_map(|s| s.parse().ok())
                .collect();
            (
                nums.first().copied().unwrap_or(0.0),
                nums.get(1).copied().unwrap_or(0.0),
                nums.get(2).copied().unwrap_or(0.0),
            )
        }
        None => (0.0, 0.0, 0.0),
    }
}

pub fn get_system_metrics(force_refresh: bool) -> Value {
    if !force_refresh {
        let guard = CACHE.lock().unwrap();
        if let Some((ref ts, ref val)) = *guard {
            if ts.elapsed().as_secs() < CACHE_TTL_SECS {
                return val.clone();
            }
        }
    }

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();
    let total_memory = parse_sysctl_int("hw.memsize");
    let vm_stat = parse_vm_stat();
    let swap = parse_swap_usage();
    let boot_time = parse_boot_time();
    let (root_total, root_used, root_free) = disk_usage("/");

    let home_dir = std::env::var("HOME").unwrap_or_else(|_| "/".to_string());
    let (home_total, home_used, home_free) = disk_usage(&home_dir);
    let (load1, load5, load15) = load_avg();

    let free_memory = vm_stat.get("free_bytes").and_then(|v| v.as_i64());
    let file_backed = vm_stat.get("file_backed_bytes").and_then(|v| v.as_i64());
    let anonymous = vm_stat.get("anonymous_bytes").and_then(|v| v.as_i64());
    let wired = vm_stat.get("wired_bytes").and_then(|v| v.as_i64());
    let compressed = vm_stat.get("compressed_bytes").and_then(|v| v.as_i64());

    let used_memory = match (total_memory, free_memory, file_backed) {
        (Some(t), Some(f), Some(c)) => Some(std::cmp::max(t - f - c, 0)),
        _ => None,
    };
    let raw_used_memory = match (total_memory, free_memory) {
        (Some(t), Some(f)) => Some(t - f),
        _ => None,
    };

    let cpu_count = std::thread::available_parallelism()
        .map(|n| n.get() as i64)
        .unwrap_or(1);

    let pid = std::process::id();
    let rss = process_rss_bytes(pid);

    let hostname = hostname::get()
        .map(|h| h.to_string_lossy().into_owned())
        .unwrap_or_default();

    let mac_ver = run_text(&["sw_vers", "-productVersion"]).unwrap_or_default();

    let metrics = json!({
        "collected_at": now,
        "hostname": hostname,
        "platform": {
            "system": "Darwin",
            "release": run_text(&["uname", "-r"]).unwrap_or_default(),
            "version": run_text(&["uname", "-v"]).unwrap_or_default(),
            "mac_ver": mac_ver,
            "machine": run_text(&["uname", "-m"]).unwrap_or_default(),
        },
        "cpu": {
            "logical_cores": cpu_count,
            "loadavg_1m": (load1 * 100.0).round() / 100.0,
            "loadavg_5m": (load5 * 100.0).round() / 100.0,
            "loadavg_15m": (load15 * 100.0).round() / 100.0,
        },
        "memory": {
            "total_bytes": total_memory,
            "used_bytes": used_memory,
            "raw_used_bytes": raw_used_memory,
            "free_bytes": free_memory,
            "cached_files_bytes": file_backed,
            "app_memory_bytes": anonymous,
            "active_bytes": vm_stat.get("active_bytes"),
            "inactive_bytes": vm_stat.get("inactive_bytes"),
            "speculative_bytes": vm_stat.get("speculative_bytes"),
            "wired_bytes": wired,
            "compressed_bytes": compressed,
            "purgeable_bytes": vm_stat.get("purgeable_bytes"),
            "file_backed_bytes": file_backed,
        },
        "swap": swap,
        "activity_monitor": {
            "memory": {
                "physical_memory_bytes": total_memory,
                "memory_used_bytes": used_memory,
                "cached_files_bytes": file_backed,
                "app_memory_bytes": anonymous,
                "wired_memory_bytes": wired,
                "compressed_bytes": compressed,
                "swap_used_bytes": swap.get("used_bytes"),
            }
        },
        "disk": {
            "root_total_bytes": root_total,
            "root_used_bytes": root_used,
            "root_free_bytes": root_free,
            "home_total_bytes": home_total,
            "home_used_bytes": home_used,
            "home_free_bytes": home_free,
        },
        "uptime_seconds": boot_time.map(|bt| (now - bt) as i64),
        "agent_process": {
            "pid": pid,
            "rss_bytes": rss,
        },
    });

    let mut guard = CACHE.lock().unwrap();
    *guard = Some((Instant::now(), metrics.clone()));
    metrics
}
