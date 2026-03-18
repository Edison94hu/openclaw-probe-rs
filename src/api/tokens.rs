use axum::extract::{Query, State};
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::collector;
use crate::config::SharedState;
use crate::database;
use crate::token_scanner::estimate_cost;
use crate::token_scanner::model_pricing_json;

fn build_agent_name_map(_state: &SharedState, config: &crate::config::AppConfig) -> std::collections::HashMap<(String, String), String> {
    let mut mapping = std::collections::HashMap::new();
    for instance in collector::collect_all_info(config) {
        let instance_name = instance.get("name").and_then(|v| v.as_str()).unwrap_or("");
        if let Some(agents) = instance.get("agents").and_then(|v| v.as_array()) {
            for agent in agents {
                let agent_id = agent.get("id").and_then(|v| v.as_str()).unwrap_or("");
                let agent_name = agent
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or(agent_id);
                if !instance_name.is_empty() && !agent_id.is_empty() {
                    mapping.insert(
                        (instance_name.to_string(), agent_id.to_string()),
                        agent_name.to_string(),
                    );
                }
            }
        }
    }
    mapping
}

pub async fn build_token_summary_payload(state: &SharedState) -> Value {
    let config = state.config.get().await;
    let mut summary = database::get_token_summary(&state.db)
        .await
        .unwrap_or_default();

    let agent_name_map = build_agent_name_map(state, &config);

    // Add estimated_cost to by_model
    if let Some(by_model) = summary.get_mut("by_model").and_then(|v| v.as_array_mut()) {
        for model_data in by_model.iter_mut() {
            let cost = estimate_cost(
                model_data.get("model").and_then(|v| v.as_str()),
                model_data.get("input_tokens").and_then(|v| v.as_i64()).unwrap_or(0),
                model_data.get("output_tokens").and_then(|v| v.as_i64()).unwrap_or(0),
                model_data.get("cache_read_tokens").and_then(|v| v.as_i64()).unwrap_or(0),
                model_data.get("cache_write_tokens").and_then(|v| v.as_i64()).unwrap_or(0),
            );
            if let Some(obj) = model_data.as_object_mut() {
                obj.insert("estimated_cost".into(), json!(cost));
            }
        }
    }

    // Calculate per-agent costs from by_agent_model
    let mut agent_costs: std::collections::HashMap<(String, String), f64> =
        std::collections::HashMap::new();
    if let Some(by_agent_model) = summary.get("by_agent_model").and_then(|v| v.as_array()) {
        for row in by_agent_model {
            let key = (
                row.get("instance_name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                row.get("agent_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
            );
            let cost = estimate_cost(
                row.get("model").and_then(|v| v.as_str()),
                row.get("input_tokens").and_then(|v| v.as_i64()).unwrap_or(0),
                row.get("output_tokens").and_then(|v| v.as_i64()).unwrap_or(0),
                row.get("cache_read_tokens").and_then(|v| v.as_i64()).unwrap_or(0),
                row.get("cache_write_tokens").and_then(|v| v.as_i64()).unwrap_or(0),
            );
            *agent_costs.entry(key).or_insert(0.0) += cost;
        }
    }

    // Add agent_name and estimated_cost to by_agent
    if let Some(by_agent) = summary.get_mut("by_agent").and_then(|v| v.as_array_mut()) {
        for agent_data in by_agent.iter_mut() {
            let key = (
                agent_data
                    .get("instance_name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                agent_data
                    .get("agent_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
            );
            if let Some(obj) = agent_data.as_object_mut() {
                let agent_name = agent_name_map
                    .get(&key)
                    .cloned()
                    .unwrap_or_else(|| key.1.clone());
                obj.insert("agent_name".into(), json!(agent_name));
                let cost = agent_costs.get(&key).copied().unwrap_or(0.0);
                obj.insert(
                    "estimated_cost".into(),
                    json!((cost * 1_000_000.0).round() / 1_000_000.0),
                );
            }
        }
    }

    // Total cost
    let total_cost: f64 = summary
        .get("by_model")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|m| m.get("estimated_cost").and_then(|v| v.as_f64()))
                .sum()
        })
        .unwrap_or(0.0);

    if let Some(obj) = summary.as_object_mut() {
        obj.insert(
            "total_estimated_cost".into(),
            json!((total_cost * 10000.0).round() / 10000.0),
        );
        obj.remove("by_agent_model");
    }

    summary
}

pub async fn build_agent_heatmap_payload(state: &SharedState, range_key: &str) -> Value {
    let config = state.config.get().await;
    let mut rows = database::get_agent_heatmap(&state.db, range_key, 8)
        .await
        .unwrap_or_default();

    let agent_name_map = build_agent_name_map(state, &config);

    for row in &mut rows {
        let key = (
            row.get("instance_name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            row.get("agent_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
        );
        if let Some(obj) = row.as_object_mut() {
            obj.insert(
                "agent_name".into(),
                json!(agent_name_map
                    .get(&key)
                    .cloned()
                    .unwrap_or_else(|| key.1.clone())),
            );
        }
    }

    json!({
        "range": range_key,
        "timezone": "Asia/Shanghai",
        "hours": (0..24).collect::<Vec<_>>(),
        "rows": rows,
    })
}

// GET /api/tokens/summary
pub async fn token_summary(State(state): State<SharedState>) -> Json<Value> {
    Json(build_token_summary_payload(&state).await)
}

#[derive(Deserialize)]
pub struct TrendQuery {
    #[serde(default = "default_days")]
    pub days: i64,
}
fn default_days() -> i64 {
    7
}

// GET /api/tokens/trend
pub async fn token_trend(
    State(state): State<SharedState>,
    Query(params): Query<TrendQuery>,
) -> Json<Value> {
    let days = params.days.clamp(1, 90);
    let mut trend = database::get_token_trend(&state.db, days)
        .await
        .unwrap_or_default();

    for day_data in &mut trend {
        let cost = estimate_cost(
            None,
            day_data.get("input_tokens").and_then(|v| v.as_i64()).unwrap_or(0),
            day_data.get("output_tokens").and_then(|v| v.as_i64()).unwrap_or(0),
            day_data.get("cache_read_tokens").and_then(|v| v.as_i64()).unwrap_or(0),
            day_data.get("cache_write_tokens").and_then(|v| v.as_i64()).unwrap_or(0),
        );
        if let Some(obj) = day_data.as_object_mut() {
            obj.insert("estimated_cost".into(), json!(cost));
        }
    }

    Json(json!({ "trend": trend, "days": days }))
}

#[derive(Deserialize)]
pub struct HeatmapQuery {
    #[serde(default = "default_range", rename = "range")]
    pub range_key: String,
}
fn default_range() -> String {
    "today".into()
}

// GET /api/tokens/agent-heatmap
pub async fn token_agent_heatmap(
    State(state): State<SharedState>,
    Query(params): Query<HeatmapQuery>,
) -> Json<Value> {
    let range_key = match params.range_key.as_str() {
        "today" | "7d" | "14d" => params.range_key.as_str(),
        _ => "today",
    };
    Json(build_agent_heatmap_payload(&state, range_key).await)
}

// GET /api/tokens/pricing
pub async fn get_pricing() -> Json<Value> {
    Json(json!({ "pricing": model_pricing_json() }))
}
