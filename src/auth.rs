use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::config::SharedState;

/// Axum middleware: allow open access when no token is configured;
/// otherwise require exact match via X-Agent-Token header or Bearer auth.
pub async fn verify_agent_token(
    state: axum::extract::State<SharedState>,
    req: Request,
    next: Next,
) -> Response {
    let config = state.config.get().await;
    let expected = config.agent.api_token.trim().to_string();

    if expected.is_empty() {
        return next.run(req).await;
    }

    let headers = req.headers();

    let presented = headers
        .get("X-Agent-Token")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim().to_string())
        .or_else(|| {
            headers
                .get("Authorization")
                .and_then(|v| v.to_str().ok())
                .and_then(|auth| {
                    if auth.to_lowercase().starts_with("bearer ") {
                        Some(auth[7..].trim().to_string())
                    } else {
                        None
                    }
                })
        })
        .unwrap_or_default();

    if presented != expected {
        return (
            StatusCode::UNAUTHORIZED,
            axum::Json(serde_json::json!({"detail": "Invalid agent token"})),
        )
            .into_response();
    }

    next.run(req).await
}
