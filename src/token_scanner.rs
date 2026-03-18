use std::collections::HashMap;
use std::io::{BufRead, Seek, SeekFrom};
use std::path::Path;

use crate::config::{expand_home, SharedState};
use crate::database;

// ---------------------------------------------------------------------------
// Model pricing per 1M tokens (USD)
// ---------------------------------------------------------------------------

pub struct ModelPricing {
    pub input: f64,
    pub output: f64,
    pub cache_read: f64,
    pub cache_write: f64,
}

pub fn model_pricing_table() -> HashMap<&'static str, ModelPricing> {
    let mut m = HashMap::new();
    m.insert("anthropic/claude-opus-4-6", ModelPricing { input: 15.0, output: 75.0, cache_read: 1.5, cache_write: 18.75 });
    m.insert("anthropic/claude-opus-4-5", ModelPricing { input: 15.0, output: 75.0, cache_read: 1.5, cache_write: 18.75 });
    m.insert("anthropic/claude-sonnet-4-6", ModelPricing { input: 3.0, output: 15.0, cache_read: 0.3, cache_write: 3.75 });
    m.insert("anthropic/claude-haiku-4-5", ModelPricing { input: 0.8, output: 4.0, cache_read: 0.08, cache_write: 1.0 });
    m.insert("openai/gpt-5.4", ModelPricing { input: 2.0, output: 10.0, cache_read: 0.5, cache_write: 1.0 });
    m.insert("openai/gpt-5.3-codex", ModelPricing { input: 2.0, output: 10.0, cache_read: 0.5, cache_write: 1.0 });
    m.insert("openai-codex/gpt-5.3-codex", ModelPricing { input: 2.0, output: 10.0, cache_read: 0.5, cache_write: 1.0 });
    m.insert("openai/gpt-4o", ModelPricing { input: 2.5, output: 10.0, cache_read: 1.25, cache_write: 2.5 });
    m.insert("openai/gpt-4o-mini", ModelPricing { input: 0.15, output: 0.6, cache_read: 0.075, cache_write: 0.15 });
    m.insert("google/gemini-2.5-pro", ModelPricing { input: 1.25, output: 10.0, cache_read: 0.315, cache_write: 1.25 });
    m
}

pub fn model_pricing_json() -> serde_json::Value {
    let table = model_pricing_table();
    let mut map = serde_json::Map::new();
    for (k, v) in &table {
        map.insert(
            k.to_string(),
            serde_json::json!({
                "input": v.input,
                "output": v.output,
                "cache_read": v.cache_read,
                "cache_write": v.cache_write,
            }),
        );
    }
    serde_json::Value::Object(map)
}

pub fn normalize_model(provider: Option<&str>, model: Option<&str>) -> Option<String> {
    let model = model?;
    if model.is_empty() {
        return None;
    }
    if model.contains('/') {
        return Some(model.to_string());
    }
    if let Some(p) = provider {
        if !p.is_empty() {
            return Some(format!("{p}/{model}"));
        }
    }
    Some(model.to_string())
}

pub fn estimate_cost(
    model: Option<&str>,
    input_tokens: i64,
    output_tokens: i64,
    cache_read_tokens: i64,
    cache_write_tokens: i64,
) -> f64 {
    let model = match model {
        Some(m) if !m.is_empty() => m,
        _ => return 0.0,
    };
    let table = model_pricing_table();

    let pricing = table.get(model).or_else(|| {
        table
            .iter()
            .find(|(k, _)| model.contains(*k) || k.contains(model))
            .map(|(_, v)| v)
    });

    match pricing {
        Some(p) => {
            let cost = input_tokens as f64 * p.input / 1_000_000.0
                + output_tokens as f64 * p.output / 1_000_000.0
                + cache_read_tokens as f64 * p.cache_read / 1_000_000.0
                + cache_write_tokens as f64 * p.cache_write / 1_000_000.0;
            (cost * 1_000_000.0).round() / 1_000_000.0
        }
        None => 0.0,
    }
}

// ---------------------------------------------------------------------------
// Incremental JSONL scanner
// ---------------------------------------------------------------------------

async fn scan_jsonl_file(
    pool: &sqlx::SqlitePool,
    instance_name: &str,
    agent_id: &str,
    filepath: &Path,
) -> anyhow::Result<()> {
    let file_key = filepath.to_string_lossy().to_string();
    let (last_offset, _) = database::get_scan_state(pool, &file_key).await?;

    let file_size = match std::fs::metadata(filepath) {
        Ok(m) => m.len() as i64,
        Err(_) => return Ok(()),
    };

    if file_size <= last_offset {
        return Ok(());
    }

    let mut count = 0i64;
    let mut new_offset = last_offset;

    let file = std::fs::File::open(filepath)?;
    let mut reader = std::io::BufReader::new(file);
    reader.seek(SeekFrom::Start(last_offset as u64))?;

    let mut line = String::new();
    loop {
        line.clear();
        let bytes_read = reader.read_line(&mut line)?;
        if bytes_read == 0 {
            break;
        }
        new_offset += bytes_read as i64;

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let entry: serde_json::Value = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(_) => continue,
        };

        let message = entry
            .get("message")
            .and_then(|v| v.as_object())
            .cloned()
            .unwrap_or_default();

        let usage = entry
            .get("responseUsage")
            .or_else(|| message.get("usage").map(|v| v))
            .and_then(|v| v.as_object());

        let usage = match usage {
            Some(u) => u,
            None => continue,
        };

        let ts = entry
            .get("timestamp")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let provider = message
            .get("provider")
            .or_else(|| entry.get("provider"))
            .or_else(|| entry.get("responseProvider"))
            .and_then(|v| v.as_str());

        let raw_model = message
            .get("model")
            .or_else(|| entry.get("model"))
            .or_else(|| entry.get("responseModel"))
            .and_then(|v| v.as_str());

        let model = normalize_model(provider, raw_model);

        let input_tokens = get_i64(usage, &["inputTokens", "input_tokens", "input"]);
        let output_tokens = get_i64(usage, &["outputTokens", "output_tokens", "output"]);
        let cache_read =
            get_i64(usage, &["cacheReadInputTokens", "cache_read_input_tokens", "cacheRead"]);
        let cache_write = get_i64(
            usage,
            &["cacheCreationInputTokens", "cache_creation_input_tokens", "cacheWrite"],
        );

        database::insert_token_usage(
            pool,
            instance_name,
            Some(agent_id),
            &file_key,
            model.as_deref(),
            &ts,
            input_tokens,
            output_tokens,
            cache_read,
            cache_write,
        )
        .await?;
        count += 1;
    }

    database::update_scan_state(pool, &file_key, new_offset).await?;
    if count > 0 {
        tracing::info!(
            "Scanned {} token entries from {}",
            count,
            filepath
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
        );
    }

    Ok(())
}

fn get_i64(obj: &serde_json::Map<String, serde_json::Value>, keys: &[&str]) -> i64 {
    for key in keys {
        if let Some(v) = obj.get(*key) {
            if let Some(n) = v.as_i64() {
                if n != 0 {
                    return n;
                }
            }
        }
    }
    0
}

async fn scan_instance_sessions(
    pool: &sqlx::SqlitePool,
    instance_name: &str,
    openclaw_home: &str,
) {
    let home = expand_home(openclaw_home);
    let agents_dir = home.join("agents");

    let entries = match std::fs::read_dir(&agents_dir) {
        Ok(e) => e,
        Err(_) => return,
    };

    for entry in entries.flatten() {
        if !entry.path().is_dir() {
            continue;
        }
        let agent_id = entry.file_name().to_string_lossy().to_string();
        let sessions_dir = entry.path().join("sessions");
        if !sessions_dir.exists() {
            continue;
        }

        let session_entries = match std::fs::read_dir(&sessions_dir) {
            Ok(e) => e,
            Err(_) => continue,
        };

        for se in session_entries.flatten() {
            let path = se.path();
            if path.extension().and_then(|e| e.to_str()) == Some("jsonl") {
                if let Err(e) = scan_jsonl_file(pool, instance_name, &agent_id, &path).await {
                    tracing::warn!("Error scanning {}: {}", path.display(), e);
                }
            }
        }
    }
}

/// Run token scan for all enabled instances.
pub async fn run_token_scan(state: &SharedState) {
    let config = state.config.get().await;
    for inst in &config.instances {
        if inst.enabled {
            scan_instance_sessions(&state.db, &inst.name, &inst.openclaw_home).await;
        }
    }
}
