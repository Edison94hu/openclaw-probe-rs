use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use serde_json::{json, Value};

use crate::config::{AppConfig, SharedState};

// GET /api/config
pub async fn read_config(State(state): State<SharedState>) -> Json<Value> {
    let config = state.config.get().await;
    Json(serde_json::to_value(&config).unwrap_or_default())
}

// PUT /api/config
pub async fn write_config(
    State(state): State<SharedState>,
    Json(data): Json<Value>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let new_config: AppConfig = serde_json::from_value(data).map_err(|e: serde_json::Error| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({"detail": e.to_string()})),
        )
    })?;

    match state.config.update(new_config).await {
        Ok(config) => Ok(Json(json!({
            "message": "配置已更新",
            "config": serde_json::to_value(&config).unwrap_or_default(),
        }))),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"detail": e.to_string()})),
        )),
    }
}
