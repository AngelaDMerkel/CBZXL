use crate::{
    config::Config,
    db::Database,
    engine::{CommandMediaBackend, Engine, MediaBackend},
    progress::ProgressHub,
};
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::RwLock;

pub async fn run(config: Config, database: Database) -> Result<()> {
    let progress = ProgressHub::at(config.runtime.clone());
    run_with(config, database, progress).await
}

pub async fn run_with(config: Config, database: Database, progress: ProgressHub) -> Result<()> {
    run_with_status(config, database, progress, Arc::new(RwLock::new(None))).await
}

pub async fn run_with_status(
    config: Config,
    database: Database,
    progress: ProgressHub,
    runtime_error: Arc<RwLock<Option<String>>>,
) -> Result<()> {
    database.recover_interrupted()?;
    let encoder_timeout = if config.encoder_timeout.is_zero() {
        config.worker_stall
    } else {
        config.encoder_timeout
    };
    let media = Arc::new(CommandMediaBackend::new(
        encoder_timeout,
        config.auxiliary_timeout,
    ));
    if let Err(error) = media.doctor() {
        *runtime_error.write().await = Some(error.to_string());
        return Err(error);
    }
    *runtime_error.write().await = None;
    let engine = Engine {
        config: config.clone(),
        database: database.clone(),
        media,
        progress,
    };
    let recovered = engine.recover_commits()?;
    if recovered > 0 {
        tracing::warn!(recovered, "recovered interrupted archive commits");
    }
    tracing::info!(poll_ms = config.poll_every.as_millis(), "worker ready");
    let mut next_maintenance = tokio::time::Instant::now();
    loop {
        database.enqueue_due_schedules(&config.timezone)?;
        if let Some(job) = database.claim()? {
            let library = database
                .libraries()?
                .into_iter()
                .find(|library| library.id == job.library_id)
                .ok_or_else(|| anyhow::anyhow!("job library is unavailable"))?;
            if let Err(error) = engine.run(&job, &library).await {
                tracing::error!(job_id = %job.id, %error, "job failed");
                if let Err(recovery_error) = engine.recover_commits() {
                    tracing::error!(job_id=%job.id,%recovery_error,"could not immediately recover interrupted archive commits");
                }
                database.finish_job(
                    &job.id,
                    crate::domain::JobStatus::Failed,
                    Some(&error.to_string()),
                )?;
            }
            if let Err(error) = crate::notifications::publish(&config, &database, &job.id).await {
                tracing::warn!(job_id=%job.id,%error,"notification delivery failed");
                database.add_event(
                    &job.id,
                    "warning",
                    "notification_failed",
                    &format!("ntfy notification failed: {error}"),
                    None,
                    None,
                )?;
            }
            continue;
        }
        if tokio::time::Instant::now() >= next_maintenance {
            if let Err(error) = database.maintenance(false, config.database_maintenance) {
                tracing::error!(%error,"database maintenance failed");
            }
            if let Err(error) = database.prune_history(config.page_outcome_retention_days) {
                tracing::error!(%error,"history pruning failed");
            }
            if let Err(error) = database.prune_transient_outcomes() {
                tracing::error!(%error,"transient outcome pruning failed");
            }
            next_maintenance = tokio::time::Instant::now() + std::time::Duration::from_secs(3600);
        }
        tokio::time::sleep(config.poll_every).await;
    }
}
