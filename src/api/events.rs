use axum::extract::{Query, State};
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::config::SharedState;
use crate::database;

#[derive(Deserialize)]
pub struct EventsQuery {
    #[serde(default = "default_limit")]
    pub limit: i64,
    #[serde(default)]
    pub offset: i64,
    pub event_type: Option<String>,
}

fn default_limit() -> i64 {
    50
}

// GET /api/events
pub async fn list_events(
    State(state): State<SharedState>,
    Query(params): Query<EventsQuery>,
) -> Json<Value> {
    let limit = params.limit.clamp(1, 500);
    let offset = params.offset.max(0);
    let events: Vec<Value> = database::get_recent_events(&state.db, limit, offset, params.event_type.as_deref())
        .await
        .unwrap_or_default();
    Json(json!({
        "events": events,
        "limit": limit,
        "offset": offset,
    }))
}
