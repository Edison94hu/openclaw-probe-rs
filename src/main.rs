mod api;
mod auth;
mod collector;
mod config;
mod database;
mod operations;
mod probe;
mod restart;
mod scheduler;
mod system_metrics;
mod token_scanner;

use std::sync::Arc;
use std::time::Instant;

use axum::middleware;
use axum::routing::{get, post, put};
use axum::Router;
use tokio::sync::RwLock;
use tower_http::cors::{Any, CorsLayer};

use config::{now_epoch, AppState, ConfigManager, SharedState};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    // Resolve paths relative to the binary or CWD
    let base_dir = std::env::current_dir()?;
    let config_path = base_dir.join("config.json");
    let db_path = base_dir.join("data").join("probe.db");

    tracing::info!("OpenClaw Probe (Rust) starting...");
    tracing::info!("Config: {}", config_path.display());
    tracing::info!("Database: {}", db_path.display());

    // Init config + database
    let config_mgr = ConfigManager::new(config_path);
    let initial_config = config_mgr.get().await;
    let pool = database::create_pool(&db_path).await?;

    let state: SharedState = Arc::new(AppState {
        config: config_mgr,
        db: pool,
        instance_states: RwLock::new(std::collections::HashMap::new()),
        started_at: Instant::now(),
        started_at_epoch: now_epoch(),
    });

    // Start background scheduler
    let shutdown_tx = scheduler::start_scheduler(state.clone());

    // Build router
    let app = build_router(state.clone());

    let bind_addr = format!(
        "{}:{}",
        initial_config.server.host, initial_config.server.port
    );
    tracing::info!("Listening on {bind_addr}");

    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;

    // Graceful shutdown on Ctrl-C
    let shutdown_signal = async move {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install CTRL+C handler");
        tracing::info!("Shutting down...");
        let _ = shutdown_tx.send(true);
    };

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal)
        .await?;

    tracing::info!("OpenClaw Probe stopped");
    Ok(())
}

fn build_router(state: SharedState) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    // Public routes (no auth)
    let public_api = Router::new()
        .route("/instances", get(api::instances::list_instances))
        .route("/instances/{name}", get(api::instances::get_instance))
        .route("/events", get(api::events::list_events))
        .route("/tokens/summary", get(api::tokens::token_summary))
        .route("/tokens/trend", get(api::tokens::token_trend))
        .route(
            "/tokens/agent-heatmap",
            get(api::tokens::token_agent_heatmap),
        )
        .route("/tokens/pricing", get(api::tokens::get_pricing))
        .route("/health", get(api::agent::agent_health));

    // Auth-protected routes
    let authed_api = Router::new()
        // Instance restart (local API style)
        .route(
            "/instances/{name}/restart",
            post(api::instances::restart_instance_api),
        )
        // Config
        .route("/config", get(api::config_api::read_config))
        .route("/config", put(api::config_api::write_config))
        // Agent endpoints
        .route("/agent/health", get(api::agent::agent_health))
        .route("/agent/snapshot", get(api::agent::agent_snapshot))
        .route(
            "/agent/instances/{name}/restart",
            post(api::agent::agent_restart_instance),
        )
        .route(
            "/agent/instances/{name}/start",
            post(api::agent::agent_start_instance),
        )
        .route(
            "/agent/instances/{name}/stop",
            post(api::agent::agent_stop_instance),
        )
        .route(
            "/agent/instances/{name}/desired-state",
            post(api::agent::agent_set_desired_state),
        )
        .route(
            "/agent/instances/{name}/lifecycle",
            get(api::agent::agent_get_instance_lifecycle),
        )
        .route(
            "/agent/maintenance/enter",
            post(api::agent::agent_enter_maintenance),
        )
        .route(
            "/agent/maintenance/exit",
            post(api::agent::agent_exit_maintenance),
        )
        .route(
            "/agent/openclaw/version",
            get(api::agent::agent_openclaw_version),
        )
        .route(
            "/agent/openclaw/latest-release",
            get(api::agent::agent_openclaw_latest_release),
        )
        .route(
            "/agent/openclaw/upgrade",
            post(api::agent::agent_upgrade_openclaw),
        )
        .route(
            "/agent/openclaw/upgrade-if-needed",
            post(api::agent::agent_upgrade_if_needed),
        )
        .route("/agent/tasks", get(api::agent::agent_list_tasks))
        .route("/agent/tasks/{task_id}", get(api::agent::agent_get_task))
        .route("/agent/backups", get(api::agent::agent_list_backups))
        .route(
            "/agent/backups/{backup_id}",
            get(api::agent::agent_get_backup),
        )
        .route(
            "/agent/backups/create",
            post(api::agent::agent_create_backup),
        )
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth::verify_agent_token,
        ));

    // Root
    let root = Router::new().route(
        "/",
        get(|| async {
            axum::Json(serde_json::json!({
                "status": "ok",
                "service": "openclaw-probe-agent",
                "message": "Agent-only mode. Use /api/agent/health or /api/agent/snapshot.",
            }))
        }),
    );

    root.nest("/api", public_api.merge(authed_api))
        .with_state(state)
        .layer(cors)
}
