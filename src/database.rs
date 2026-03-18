use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};
use std::path::Path;
use std::str::FromStr;

use crate::config::now_iso;

// ---------------------------------------------------------------------------
// Pool creation + schema init
// ---------------------------------------------------------------------------

pub async fn create_pool(db_path: &Path) -> anyhow::Result<SqlitePool> {
    if let Some(parent) = db_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let url = format!("sqlite:{}?mode=rwc", db_path.display());
    let opts = SqliteConnectOptions::from_str(&url)?
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
        .create_if_missing(true);

    let pool = SqlitePoolOptions::new()
        .max_connections(4)
        .connect_with(opts)
        .await?;

    init_tables(&pool).await?;
    Ok(pool)
}

async fn init_tables(pool: &SqlitePool) -> anyhow::Result<()> {
    sqlx::raw_sql(
        r#"
        CREATE TABLE IF NOT EXISTS probe_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            instance_name TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            status_code INTEGER,
            response_time_ms REAL,
            success INTEGER NOT NULL,
            error TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_probe_instance_ts
            ON probe_results(instance_name, timestamp DESC);

        CREATE TABLE IF NOT EXISTS restart_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            instance_name TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            trigger_reason TEXT,
            success INTEGER NOT NULL,
            exit_code INTEGER,
            stderr_output TEXT,
            health_after INTEGER
        );
        CREATE INDEX IF NOT EXISTS idx_restart_instance_ts
            ON restart_events(instance_name, timestamp DESC);

        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            event_type TEXT NOT NULL,
            instance_name TEXT,
            message TEXT NOT NULL,
            details TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_events_ts
            ON events(timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_events_type
            ON events(event_type, timestamp DESC);

        CREATE TABLE IF NOT EXISTS token_usage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            instance_name TEXT NOT NULL,
            agent_id TEXT,
            session_file TEXT NOT NULL,
            model TEXT,
            timestamp TEXT NOT NULL,
            input_tokens INTEGER DEFAULT 0,
            output_tokens INTEGER DEFAULT 0,
            cache_read_tokens INTEGER DEFAULT 0,
            cache_write_tokens INTEGER DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_token_instance_ts
            ON token_usage(instance_name, timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_token_model
            ON token_usage(model, timestamp DESC);

        CREATE TABLE IF NOT EXISTS token_scan_state (
            session_file TEXT PRIMARY KEY,
            last_offset INTEGER DEFAULT 0,
            last_scan TEXT
        );

        CREATE TABLE IF NOT EXISTS instance_lifecycle (
            instance_name TEXT PRIMARY KEY,
            desired_state TEXT NOT NULL DEFAULT 'running',
            updated_at TEXT NOT NULL,
            last_action TEXT,
            last_action_status TEXT,
            last_action_at TEXT,
            last_action_message TEXT
        );

        CREATE TABLE IF NOT EXISTS agent_settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        "#,
    )
    .execute(pool)
    .await?;

    Ok(())
}

// ---------------------------------------------------------------------------
// probe_results
// ---------------------------------------------------------------------------

pub async fn insert_probe_result(
    pool: &SqlitePool,
    instance_name: &str,
    status_code: Option<u16>,
    response_time_ms: Option<f64>,
    success: bool,
    error: Option<&str>,
) -> anyhow::Result<()> {
    let now = now_iso();
    let sc = status_code.map(|v| v as i64);
    let ok: i32 = if success { 1 } else { 0 };
    sqlx::query(
        "INSERT INTO probe_results (instance_name, timestamp, status_code, response_time_ms, success, error) VALUES (?,?,?,?,?,?)"
    )
    .bind(instance_name)
    .bind(&now)
    .bind(sc)
    .bind(response_time_ms)
    .bind(ok)
    .bind(error)
    .execute(pool)
    .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// restart_events
// ---------------------------------------------------------------------------

pub async fn insert_restart_event(
    pool: &SqlitePool,
    instance_name: &str,
    trigger_reason: &str,
    success: bool,
    exit_code: Option<i32>,
    stderr_output: Option<&str>,
    health_after: Option<bool>,
) -> anyhow::Result<()> {
    let now = now_iso();
    let ok: i32 = if success { 1 } else { 0 };
    let ha = health_after.map(|v| if v { 1i32 } else { 0 });
    sqlx::query(
        "INSERT INTO restart_events (instance_name, timestamp, trigger_reason, success, exit_code, stderr_output, health_after) VALUES (?,?,?,?,?,?,?)"
    )
    .bind(instance_name)
    .bind(&now)
    .bind(trigger_reason)
    .bind(ok)
    .bind(exit_code)
    .bind(stderr_output)
    .bind(ha)
    .execute(pool)
    .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// events
// ---------------------------------------------------------------------------

pub async fn insert_event(
    pool: &SqlitePool,
    event_type: &str,
    message: &str,
    instance_name: Option<&str>,
    details: Option<&str>,
) -> anyhow::Result<()> {
    let now = now_iso();
    sqlx::query(
        "INSERT INTO events (timestamp, event_type, instance_name, message, details) VALUES (?,?,?,?,?)"
    )
    .bind(&now)
    .bind(event_type)
    .bind(instance_name)
    .bind(message)
    .bind(details)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn get_recent_events(
    pool: &SqlitePool,
    limit: i64,
    offset: i64,
    event_type: Option<&str>,
) -> anyhow::Result<Vec<serde_json::Value>> {
    let rows = if let Some(et) = event_type {
        sqlx::query(
            "SELECT id, timestamp, event_type, instance_name, message, details FROM events WHERE event_type = ? ORDER BY timestamp DESC LIMIT ? OFFSET ?"
        )
        .bind(et)
        .bind(limit)
        .bind(offset)
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query(
            "SELECT id, timestamp, event_type, instance_name, message, details FROM events ORDER BY timestamp DESC LIMIT ? OFFSET ?"
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(pool)
        .await?
    };

    let result: Vec<serde_json::Value> = rows
        .iter()
        .map(|r| {
            serde_json::json!({
                "id": r.get::<i64, _>("id"),
                "timestamp": r.get::<String, _>("timestamp"),
                "event_type": r.get::<String, _>("event_type"),
                "instance_name": r.get::<Option<String>, _>("instance_name"),
                "message": r.get::<String, _>("message"),
                "details": r.get::<Option<String>, _>("details"),
            })
        })
        .collect();
    Ok(result)
}

// ---------------------------------------------------------------------------
// probe_results queries
// ---------------------------------------------------------------------------

pub async fn get_probe_results(
    pool: &SqlitePool,
    instance_name: &str,
    limit: i64,
) -> anyhow::Result<Vec<serde_json::Value>> {
    let rows = sqlx::query(
        "SELECT id, instance_name, timestamp, status_code, response_time_ms, success, error FROM probe_results WHERE instance_name = ? ORDER BY timestamp DESC LIMIT ?"
    )
    .bind(instance_name)
    .bind(limit)
    .fetch_all(pool)
    .await?;

    let result: Vec<serde_json::Value> = rows
        .iter()
        .map(|r| {
            serde_json::json!({
                "id": r.get::<i64, _>("id"),
                "instance_name": r.get::<String, _>("instance_name"),
                "timestamp": r.get::<String, _>("timestamp"),
                "status_code": r.get::<Option<i64>, _>("status_code"),
                "response_time_ms": r.get::<Option<f64>, _>("response_time_ms"),
                "success": r.get::<i32, _>("success"),
                "error": r.get::<Option<String>, _>("error"),
            })
        })
        .collect();
    Ok(result)
}

// ---------------------------------------------------------------------------
// restart_events queries
// ---------------------------------------------------------------------------

pub async fn get_restart_events(
    pool: &SqlitePool,
    instance_name: Option<&str>,
    window_minutes: Option<u32>,
) -> anyhow::Result<Vec<serde_json::Value>> {
    let mut query = String::from(
        "SELECT id, instance_name, timestamp, trigger_reason, success, exit_code, stderr_output, health_after FROM restart_events",
    );
    let mut conditions = Vec::new();
    if instance_name.is_some() {
        conditions.push("instance_name = ?".to_string());
    }
    if let Some(mins) = window_minutes {
        conditions.push(format!("timestamp >= datetime('now', '-{mins} minutes')"));
    }
    if !conditions.is_empty() {
        query.push_str(" WHERE ");
        query.push_str(&conditions.join(" AND "));
    }
    query.push_str(" ORDER BY timestamp DESC");

    let mut q = sqlx::query(&query);
    if let Some(name) = instance_name {
        q = q.bind(name);
    }
    let rows = q.fetch_all(pool).await?;

    let result: Vec<serde_json::Value> = rows
        .iter()
        .map(|r| {
            serde_json::json!({
                "id": r.get::<i64, _>("id"),
                "instance_name": r.get::<String, _>("instance_name"),
                "timestamp": r.get::<String, _>("timestamp"),
                "trigger_reason": r.get::<Option<String>, _>("trigger_reason"),
                "success": r.get::<i32, _>("success"),
                "exit_code": r.get::<Option<i32>, _>("exit_code"),
                "stderr_output": r.get::<Option<String>, _>("stderr_output"),
                "health_after": r.get::<Option<i32>, _>("health_after"),
            })
        })
        .collect();
    Ok(result)
}

// ---------------------------------------------------------------------------
// token_usage
// ---------------------------------------------------------------------------

pub async fn insert_token_usage(
    pool: &SqlitePool,
    instance_name: &str,
    agent_id: Option<&str>,
    session_file: &str,
    model: Option<&str>,
    timestamp: &str,
    input_tokens: i64,
    output_tokens: i64,
    cache_read_tokens: i64,
    cache_write_tokens: i64,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO token_usage (instance_name, agent_id, session_file, model, timestamp, input_tokens, output_tokens, cache_read_tokens, cache_write_tokens) VALUES (?,?,?,?,?,?,?,?,?)"
    )
    .bind(instance_name)
    .bind(agent_id)
    .bind(session_file)
    .bind(model)
    .bind(timestamp)
    .bind(input_tokens)
    .bind(output_tokens)
    .bind(cache_read_tokens)
    .bind(cache_write_tokens)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn get_scan_state(
    pool: &SqlitePool,
    session_file: &str,
) -> anyhow::Result<(i64, Option<String>)> {
    let row = sqlx::query(
        "SELECT last_offset, last_scan FROM token_scan_state WHERE session_file = ?",
    )
    .bind(session_file)
    .fetch_optional(pool)
    .await?;

    match row {
        Some(r) => Ok((
            r.get::<i64, _>("last_offset"),
            r.get::<Option<String>, _>("last_scan"),
        )),
        None => Ok((0, None)),
    }
}

pub async fn update_scan_state(
    pool: &SqlitePool,
    session_file: &str,
    offset: i64,
) -> anyhow::Result<()> {
    let now = now_iso();
    sqlx::query(
        "INSERT INTO token_scan_state (session_file, last_offset, last_scan) VALUES (?,?,?) ON CONFLICT(session_file) DO UPDATE SET last_offset=?, last_scan=?"
    )
    .bind(session_file)
    .bind(offset)
    .bind(&now)
    .bind(offset)
    .bind(&now)
    .execute(pool)
    .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// token_summary / trend / heatmap
// ---------------------------------------------------------------------------

pub async fn get_token_summary(pool: &SqlitePool) -> anyhow::Result<serde_json::Value> {
    // Overall totals
    let totals_row = sqlx::query(
        "SELECT COALESCE(SUM(input_tokens),0) as total_input, COALESCE(SUM(output_tokens),0) as total_output, COALESCE(SUM(cache_read_tokens),0) as total_cache_read, COALESCE(SUM(cache_write_tokens),0) as total_cache_write FROM token_usage"
    )
    .fetch_one(pool)
    .await?;

    let totals = serde_json::json!({
        "total_input": totals_row.get::<i64, _>("total_input"),
        "total_output": totals_row.get::<i64, _>("total_output"),
        "total_cache_read": totals_row.get::<i64, _>("total_cache_read"),
        "total_cache_write": totals_row.get::<i64, _>("total_cache_write"),
    });

    // By model
    let by_model_rows = sqlx::query(
        "SELECT model, SUM(input_tokens) as input_tokens, SUM(output_tokens) as output_tokens, SUM(cache_read_tokens) as cache_read_tokens, SUM(cache_write_tokens) as cache_write_tokens FROM token_usage GROUP BY model ORDER BY SUM(input_tokens + output_tokens) DESC"
    )
    .fetch_all(pool)
    .await?;
    let by_model: Vec<serde_json::Value> = by_model_rows
        .iter()
        .map(|r| {
            serde_json::json!({
                "model": r.get::<Option<String>, _>("model"),
                "input_tokens": r.get::<i64, _>("input_tokens"),
                "output_tokens": r.get::<i64, _>("output_tokens"),
                "cache_read_tokens": r.get::<i64, _>("cache_read_tokens"),
                "cache_write_tokens": r.get::<i64, _>("cache_write_tokens"),
            })
        })
        .collect();

    // By agent
    let by_agent_rows = sqlx::query(
        "SELECT instance_name, agent_id, SUM(input_tokens) as input_tokens, SUM(output_tokens) as output_tokens, SUM(cache_read_tokens) as cache_read_tokens, SUM(cache_write_tokens) as cache_write_tokens FROM token_usage GROUP BY instance_name, agent_id ORDER BY SUM(input_tokens + output_tokens + cache_read_tokens + cache_write_tokens) DESC"
    )
    .fetch_all(pool)
    .await?;
    let by_agent: Vec<serde_json::Value> = by_agent_rows
        .iter()
        .map(|r| {
            serde_json::json!({
                "instance_name": r.get::<String, _>("instance_name"),
                "agent_id": r.get::<Option<String>, _>("agent_id"),
                "input_tokens": r.get::<i64, _>("input_tokens"),
                "output_tokens": r.get::<i64, _>("output_tokens"),
                "cache_read_tokens": r.get::<i64, _>("cache_read_tokens"),
                "cache_write_tokens": r.get::<i64, _>("cache_write_tokens"),
            })
        })
        .collect();

    // By agent model (for cost calculation)
    let by_agent_model_rows = sqlx::query(
        "SELECT instance_name, agent_id, model, SUM(input_tokens) as input_tokens, SUM(output_tokens) as output_tokens, SUM(cache_read_tokens) as cache_read_tokens, SUM(cache_write_tokens) as cache_write_tokens FROM token_usage GROUP BY instance_name, agent_id, model"
    )
    .fetch_all(pool)
    .await?;
    let by_agent_model: Vec<serde_json::Value> = by_agent_model_rows
        .iter()
        .map(|r| {
            serde_json::json!({
                "instance_name": r.get::<String, _>("instance_name"),
                "agent_id": r.get::<Option<String>, _>("agent_id"),
                "model": r.get::<Option<String>, _>("model"),
                "input_tokens": r.get::<i64, _>("input_tokens"),
                "output_tokens": r.get::<i64, _>("output_tokens"),
                "cache_read_tokens": r.get::<i64, _>("cache_read_tokens"),
                "cache_write_tokens": r.get::<i64, _>("cache_write_tokens"),
            })
        })
        .collect();

    // Today total
    let today_row = sqlx::query(
        "SELECT COALESCE(SUM(input_tokens + output_tokens + cache_read_tokens + cache_write_tokens),0) as today_total FROM token_usage WHERE date(timestamp) = date('now')"
    )
    .fetch_one(pool)
    .await?;
    let today_total = today_row.get::<i64, _>("today_total");

    Ok(serde_json::json!({
        "totals": totals,
        "by_model": by_model,
        "by_agent": by_agent,
        "by_agent_model": by_agent_model,
        "today_total": today_total,
    }))
}

pub async fn get_token_trend(
    pool: &SqlitePool,
    days: i64,
) -> anyhow::Result<Vec<serde_json::Value>> {
    let offset = format!("-{days} days");
    let rows = sqlx::query(
        "SELECT date(timestamp) as date, SUM(input_tokens) as input_tokens, SUM(output_tokens) as output_tokens, SUM(cache_read_tokens) as cache_read_tokens, SUM(cache_write_tokens) as cache_write_tokens FROM token_usage WHERE timestamp >= datetime('now', ?) GROUP BY date(timestamp) ORDER BY date(timestamp)"
    )
    .bind(&offset)
    .fetch_all(pool)
    .await?;

    let result: Vec<serde_json::Value> = rows
        .iter()
        .map(|r| {
            serde_json::json!({
                "date": r.get::<String, _>("date"),
                "input_tokens": r.get::<i64, _>("input_tokens"),
                "output_tokens": r.get::<i64, _>("output_tokens"),
                "cache_read_tokens": r.get::<i64, _>("cache_read_tokens"),
                "cache_write_tokens": r.get::<i64, _>("cache_write_tokens"),
            })
        })
        .collect();
    Ok(result)
}

pub async fn get_agent_heatmap(
    pool: &SqlitePool,
    range_key: &str,
    tz_offset_hours: i32,
) -> anyhow::Result<Vec<serde_json::Value>> {
    let ts_expr = format!(
        "datetime(replace(replace(timestamp, 'T', ' '), 'Z', ''), '{tz_offset_hours:+} hours')"
    );

    let where_clause = match range_key {
        "7d" => format!(
            "{ts_expr} >= datetime('now', '{tz_offset_hours:+} hours', '-7 days')"
        ),
        "14d" => format!(
            "{ts_expr} >= datetime('now', '{tz_offset_hours:+} hours', '-14 days')"
        ),
        _ => format!(
            "date({ts_expr}) = date(datetime('now', '{tz_offset_hours:+} hours'))"
        ),
    };

    let sql = format!(
        "SELECT instance_name, agent_id, \
         CAST(strftime('%H', {ts_expr}) AS INTEGER) as hour, \
         SUM(input_tokens + output_tokens + cache_read_tokens + cache_write_tokens) as total_tokens, \
         COUNT(*) as event_count, \
         COUNT(DISTINCT date({ts_expr})) as active_days \
         FROM token_usage \
         WHERE {where_clause} \
         GROUP BY instance_name, agent_id, hour \
         ORDER BY agent_id, hour"
    );

    let rows = sqlx::query(&sql).fetch_all(pool).await?;
    let result: Vec<serde_json::Value> = rows
        .iter()
        .map(|r| {
            serde_json::json!({
                "instance_name": r.get::<String, _>("instance_name"),
                "agent_id": r.get::<Option<String>, _>("agent_id"),
                "hour": r.get::<i32, _>("hour"),
                "total_tokens": r.get::<i64, _>("total_tokens"),
                "event_count": r.get::<i64, _>("event_count"),
                "active_days": r.get::<i64, _>("active_days"),
            })
        })
        .collect();
    Ok(result)
}

// ---------------------------------------------------------------------------
// instance_lifecycle
// ---------------------------------------------------------------------------

pub async fn get_instance_lifecycle(
    pool: &SqlitePool,
    instance_name: &str,
) -> anyhow::Result<serde_json::Value> {
    let row = sqlx::query(
        "SELECT instance_name, desired_state, updated_at, last_action, last_action_status, last_action_at, last_action_message FROM instance_lifecycle WHERE instance_name = ?"
    )
    .bind(instance_name)
    .fetch_optional(pool)
    .await?;

    if let Some(r) = row {
        return Ok(serde_json::json!({
            "instance_name": r.get::<String, _>("instance_name"),
            "desired_state": r.get::<String, _>("desired_state"),
            "updated_at": r.get::<String, _>("updated_at"),
            "last_action": r.get::<Option<String>, _>("last_action"),
            "last_action_status": r.get::<Option<String>, _>("last_action_status"),
            "last_action_at": r.get::<Option<String>, _>("last_action_at"),
            "last_action_message": r.get::<Option<String>, _>("last_action_message"),
        }));
    }

    // Insert default
    let now = now_iso();
    sqlx::query(
        "INSERT INTO instance_lifecycle (instance_name, desired_state, updated_at) VALUES (?, 'running', ?)",
    )
    .bind(instance_name)
    .bind(&now)
    .execute(pool)
    .await?;

    Ok(serde_json::json!({
        "instance_name": instance_name,
        "desired_state": "running",
        "updated_at": now,
        "last_action": null,
        "last_action_status": null,
        "last_action_at": null,
        "last_action_message": null,
    }))
}

pub async fn set_instance_desired_state(
    pool: &SqlitePool,
    instance_name: &str,
    desired_state: &str,
    action: &str,
    status: &str,
    message: Option<&str>,
) -> anyhow::Result<()> {
    let now = now_iso();
    sqlx::query(
        "INSERT INTO instance_lifecycle (instance_name, desired_state, updated_at, last_action, last_action_status, last_action_at, last_action_message) \
         VALUES (?,?,?,?,?,?,?) \
         ON CONFLICT(instance_name) DO UPDATE SET \
         desired_state = excluded.desired_state, \
         updated_at = excluded.updated_at, \
         last_action = excluded.last_action, \
         last_action_status = excluded.last_action_status, \
         last_action_at = excluded.last_action_at, \
         last_action_message = excluded.last_action_message"
    )
    .bind(instance_name)
    .bind(desired_state)
    .bind(&now)
    .bind(action)
    .bind(status)
    .bind(&now)
    .bind(message)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn record_instance_lifecycle_action(
    pool: &SqlitePool,
    instance_name: &str,
    action: &str,
    status: &str,
    message: Option<&str>,
) -> anyhow::Result<()> {
    let now = now_iso();
    sqlx::query(
        "UPDATE instance_lifecycle SET last_action = ?, last_action_status = ?, last_action_at = ?, last_action_message = ? WHERE instance_name = ?"
    )
    .bind(action)
    .bind(status)
    .bind(&now)
    .bind(message)
    .bind(instance_name)
    .execute(pool)
    .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// agent_settings / maintenance_mode
// ---------------------------------------------------------------------------

pub async fn get_agent_setting(
    pool: &SqlitePool,
    key: &str,
    default: Option<&str>,
) -> anyhow::Result<Option<String>> {
    let row = sqlx::query("SELECT value FROM agent_settings WHERE key = ?")
        .bind(key)
        .fetch_optional(pool)
        .await?;
    match row {
        Some(r) => Ok(Some(r.get::<String, _>("value"))),
        None => Ok(default.map(|s| s.to_string())),
    }
}

pub async fn set_agent_setting(
    pool: &SqlitePool,
    key: &str,
    value: &str,
) -> anyhow::Result<()> {
    let now = now_iso();
    sqlx::query(
        "INSERT INTO agent_settings (key, value, updated_at) VALUES (?,?,?) ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at"
    )
    .bind(key)
    .bind(value)
    .bind(&now)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn get_maintenance_mode(pool: &SqlitePool) -> anyhow::Result<bool> {
    let val = get_agent_setting(pool, "maintenance_mode", Some("false")).await?;
    Ok(val.as_deref() == Some("true"))
}

pub async fn set_maintenance_mode(pool: &SqlitePool, enabled: bool) -> anyhow::Result<()> {
    set_agent_setting(
        pool,
        "maintenance_mode",
        if enabled { "true" } else { "false" },
    )
    .await
}
