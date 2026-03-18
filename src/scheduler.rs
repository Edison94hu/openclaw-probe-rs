use crate::config::SharedState;
use crate::database;
use crate::probe;
use crate::restart;
use crate::token_scanner;
use crate::collector;

use tokio::sync::watch;

/// Start all background loops. Returns a shutdown sender.
pub fn start_scheduler(state: SharedState) -> watch::Sender<bool> {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Probe loop
    {
        let s = state.clone();
        let mut rx = shutdown_rx.clone();
        tokio::spawn(async move {
            tracing::info!("Probe loop started");
            loop {
                let config = s.config.get().await;
                if let Err(e) = tokio::select! {
                    _ = rx.changed() => break,
                    result = async {
                        probe::run_probe_cycle(&s).await;

                        let maintenance: bool = database::get_maintenance_mode(&s.db)
                            .await
                            .unwrap_or(false);

                        for inst in &config.instances {
                            if !inst.enabled {
                                continue;
                            }
                            let lifecycle: serde_json::Value = database::get_instance_lifecycle(&s.db, &inst.name)
                                .await
                                .unwrap_or_default();
                            let desired = lifecycle.get("desired_state")
                                .and_then(|v: &serde_json::Value| v.as_str())
                                .unwrap_or("running");
                            if desired != "running" {
                                continue;
                            }

                            let (online, failures) = {
                                let states = s.instance_states.read().await;
                                states.get(&inst.name)
                                    .map(|st| (st.online, st.consecutive_failures))
                                    .unwrap_or((None, 0))
                            };

                            if online == Some(false) && failures >= config.probe.failure_threshold {
                                if maintenance {
                                    continue;
                                }
                                // Attempt auto-restart
                                if config.restart.enabled {
                                    let can_restart = restart::should_restart(&s, inst).await;
                                    if can_restart {
                                        let success = restart::restart_instance(&s, inst).await;
                                        if !success {
                                            tracing::warn!("Auto-restart failed for {}", inst.name);
                                        }
                                    }
                                }
                            }
                        }
                        Ok::<(), anyhow::Error>(())
                    } => result,
                } {
                    tracing::error!("Probe cycle error: {e}");
                }

                tokio::select! {
                    _ = rx.changed() => break,
                    _ = tokio::time::sleep(std::time::Duration::from_secs(config.probe.interval_seconds)) => {}
                }
            }
            tracing::info!("Probe loop stopped");
        });
    }

    // Token scan loop
    {
        let s = state.clone();
        let mut rx = shutdown_rx.clone();
        tokio::spawn(async move {
            tracing::info!("Token scan loop started");
            loop {
                let config = s.config.get().await;
                token_scanner::run_token_scan(&s).await;

                tokio::select! {
                    _ = rx.changed() => break,
                    _ = tokio::time::sleep(std::time::Duration::from_secs(config.token_scan.interval_minutes * 60)) => {}
                }
            }
            tracing::info!("Token scan loop stopped");
        });
    }

    // Collector loop
    {
        let s = state.clone();
        let mut rx = shutdown_rx.clone();
        tokio::spawn(async move {
            tracing::info!("Collector loop started");
            loop {
                let config = s.config.get().await;
                let _ = collector::collect_all_info(&config);

                tokio::select! {
                    _ = rx.changed() => break,
                    _ = tokio::time::sleep(std::time::Duration::from_secs(60)) => {}
                }
            }
            tracing::info!("Collector loop stopped");
        });
    }

    tracing::info!("Scheduler started");
    shutdown_tx
}
