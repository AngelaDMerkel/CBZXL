mod archive;
mod media;
mod scan;

pub use media::{CommandMediaBackend, ImageInfo, MediaBackend, MediaCandidate, MediaRequest};

pub fn read_archive_entry(source: &std::path::Path, entry: &str, limit: u64) -> Result<Vec<u8>> {
    archive::entry_bytes(source, entry, limit)
}

pub fn inspect_salvage(source: &std::path::Path, limit: u64) -> serde_json::Value {
    let inspect = |partial| -> Result<Vec<String>> {
        let root = tempfile::tempdir()?;
        archive::extract_salvage(source, root.path(), limit, partial)?;
        let mut entries = walkdir::WalkDir::new(root.path())
            .into_iter()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_type().is_file())
            .filter_map(|entry| {
                entry
                    .path()
                    .strip_prefix(root.path())
                    .ok()
                    .map(|path| path.to_string_lossy().replace('\\', "/"))
            })
            .collect::<Vec<_>>();
        entries.sort();
        Ok(entries)
    };
    match inspect(false) {
        Ok(entries) => {
            let rows=entries.iter().map(|entry|serde_json::json!({"entry_path":entry,"status":"recoverable","repairable":true,"image_readable":true})).collect::<Vec<_>>();
            serde_json::json!({"kind":"salvage","repairable":true,"salvageable":true,"partial_salvageable":false,"entries":rows,"recovered_entries":entries.len(),"checked_entries":entries.len(),"verified_count":entries.len(),"recoverable_count":entries.len(),"needs_decision":0,"needs_decision_count":0})
        }
        Err(complete_error) => match inspect(true) {
            Ok(entries) => {
                let rows=entries.iter().map(|entry|serde_json::json!({"entry_path":entry,"status":"recoverable","repairable":true,"image_readable":true})).collect::<Vec<_>>();
                serde_json::json!({"kind":"salvage","repairable":false,"salvageable":false,"partial_salvageable":!entries.is_empty(),"entries":rows,"recovered_entries":entries.len(),"checked_entries":entries.len()+1,"verified_count":entries.len(),"recoverable_count":entries.len(),"needs_decision":1,"needs_decision_count":1,"detail":complete_error.to_string()})
            }
            Err(partial_error) => {
                serde_json::json!({"kind":"salvage","repairable":false,"salvageable":false,"partial_salvageable":false,"entries":[],"recovered_entries":0,"checked_entries":0,"verified_count":0,"recoverable_count":0,"needs_decision":1,"needs_decision_count":1,"detail":format!("{complete_error:#}; {partial_error:#}")})
            }
        },
    }
}

use crate::{
    config::Config,
    db::Database,
    domain::{ArchiveResult, Job, JobStatus, Library, Outcome, RecordedArchive},
    progress::ProgressHub,
};
use anyhow::Result;
use serde_json::json;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

pub struct Engine {
    pub config: Config,
    pub database: Database,
    pub media: Arc<dyn MediaBackend>,
    pub progress: ProgressHub,
}

struct PreparedQueue {
    archives: Vec<scan::DiscoveredArchive>,
    states: HashMap<String, crate::db::ArchiveState>,
}

impl Engine {
    pub fn recover_commits(&self) -> Result<usize> {
        let libraries = self.database.libraries()?;
        let mut recovered = 0;
        for intent in self.database.pending_commit_intents()? {
            let Some(library) = libraries
                .iter()
                .find(|library| library.id == intent.library_id)
            else {
                continue;
            };
            let path = library.resolve(&intent.path)?;
            let current = archive::fingerprint(&path).ok();
            if current.as_deref() == Some(&intent.input_fingerprint) {
                self.database
                    .clear_commit_intent(&intent.library_id, &intent.path)?;
                continue;
            }
            let record = if current.as_deref() == Some(&intent.candidate_fingerprint) {
                if let Some(value) = intent.payload.get("record") {
                    let mut record: RecordedArchive = serde_json::from_value(value.clone())?;
                    record.mtime_ns = path
                        .metadata()
                        .ok()
                        .and_then(|metadata| metadata.modified().ok())
                        .and_then(|modified| {
                            modified
                                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                                .ok()
                        })
                        .map(|duration| duration.as_nanos().min(i64::MAX as u128) as i64);
                    record
                } else {
                    let source_size = intent
                        .payload
                        .get("source_size")
                        .and_then(|value| value.as_u64())
                        .unwrap_or_else(|| path.metadata().map(|value| value.len()).unwrap_or(0));
                    let final_size = intent
                        .payload
                        .get("final_size")
                        .and_then(|value| value.as_u64())
                        .unwrap_or_else(|| path.metadata().map(|value| value.len()).unwrap_or(0));
                    RecordedArchive {
                        result: ArchiveResult {
                            path: intent.path.clone(),
                            outcome: Outcome::Optimized,
                            source_size,
                            final_size,
                            pages: 0,
                            converted: 0,
                            retained: 0,
                            failed: 0,
                            duration_ms: 0,
                            fingerprint: Some(intent.candidate_fingerprint.clone()),
                            error: None,
                        },
                        refresh_identity: false,
                        mtime_ns: None,
                        input_fingerprint: Some(intent.input_fingerprint.clone()),
                        final_fingerprint: Some(intent.candidate_fingerprint.clone()),
                        mode: Some("Recovered interrupted commit".into()),
                        effort: None,
                        pages: Vec::new(),
                        issues: Vec::new(),
                        formats: Vec::new(),
                    }
                }
            } else {
                archive::failure(
                    &path,
                    intent.path.clone(),
                    anyhow::anyhow!(
                        "interrupted replacement matches neither source nor verified candidate"
                    ),
                )
            };
            self.database
                .record_batch(&intent.job_id, &intent.library_id, &[record])?;
            recovered += 1;
        }
        Ok(recovered)
    }

    pub async fn run(&self, job: &Job, library: &Library) -> Result<()> {
        let database = self.database.clone();
        let progress = self.progress.clone();
        let preparation_job = job.clone();
        let preparation_library = library.clone();
        let Some(PreparedQueue {
            archives: paths,
            mut states,
        }) = tokio::task::spawn_blocking(move || {
            prepare_queue(&database, &progress, &preparation_job, &preparation_library)
        })
        .await??
        else {
            self.database
                .finish_job(&job.id, JobStatus::Cancelled, None)?;
            self.progress.publish(&job.id, "job_cancelled", json!({}));
            return Ok(());
        };
        let total_archives = paths.len();
        self.database.start_job(&job.id, total_archives)?;
        let job_started = std::time::Instant::now();
        self.database.add_event(
            &job.id,
            "info",
            "job_started",
            "Job started",
            None,
            Some(&json!({"archives_found":paths.len()})),
        )?;
        self.progress.publish(
            &job.id,
            "job_started",
            json!({"archives_found":paths.len()}),
        );
        let mut completed = 0_usize;
        let mut batch: Vec<RecordedArchive> =
            Vec::with_capacity(self.config.durable_checkpoint_archives.min(10_000));
        let mut last_checkpoint = std::time::Instant::now();
        let mut last_cancellation_check =
            std::time::Instant::now() - std::time::Duration::from_secs(1);
        let mut failures = 0;

        for archive in paths {
            if last_cancellation_check.elapsed() >= std::time::Duration::from_millis(250) {
                if self.database.cancellation_requested(&job.id)? {
                    flush(&self.database, job, &mut batch)?;
                    self.database
                        .finish_job(&job.id, JobStatus::Cancelled, None)?;
                    self.progress.publish(&job.id, "job_cancelled", json!({}));
                    return Ok(());
                }
                last_cancellation_check = std::time::Instant::now();
            }
            let path = archive.path;
            let relative = archive.relative;
            let state = states.remove(&relative);
            self.progress
                .publish(&job.id, "archive_started", json!({"archive":&relative}));
            let mut operation = Box::pin(archive::process_known(
                self,
                job,
                library,
                &path,
                &relative,
                state.clone(),
            ));
            let mut record = loop {
                tokio::select! {
                    result = &mut operation => break match result {
                        Ok(record) => record,
                        Err(error) if self.config.auto_retry_attempts > 0 && archive::retryable(&error) => {
                            tracing::warn!(archive=%relative,%error,"retrying archive with compatibility settings");
                            self.database.add_event(&job.id,"warning","archive_retrying","Retrying with compatibility settings",Some(&relative),Some(&json!({"error":error.to_string(),"attempt":1})))?;
                            self.progress.publish(&job.id,"archive_retrying",json!({"archive":&relative,"attempt":1}));
                            match archive::process_compatibility_known(self, job, library, &path, &relative, state.clone()).await {
                                Ok(record) => record,
                                Err(retry_error) => archive::failure(&path, relative.clone(), retry_error),
                            }
                        }
                        Err(error) => archive::failure(&path, relative.clone(), error),
                    },
                    _ = tokio::time::sleep(std::time::Duration::from_millis(500)) => {
                        if self.database.cancellation_requested(&job.id)? {
                            flush(&self.database, job, &mut batch)?;
                            self.database.finish_job(&job.id, JobStatus::Cancelled, None)?;
                            self.progress.publish(&job.id, "job_cancelled", json!({}));
                            return Ok(());
                        }
                    }
                }
            };
            if !matches!(
                record.result.outcome,
                Outcome::AlreadyCurrent | Outcome::Deferred | Outcome::Deleted
            ) {
                record.formats = archive::inventory(&path).unwrap_or_default();
            }
            if record.result.failed > 0 || matches!(record.result.outcome, Outcome::Failed) {
                failures += 1;
            }
            self.progress.publish(
                &job.id,
                "archive_completed",
                json!({
                    "archive": &relative,
                    "outcome": record.result.outcome,
                    "bytes_saved": record.result.bytes_saved(),
                }),
            );
            completed += 1;
            let elapsed = job_started.elapsed().as_secs_f64();
            let rate = if elapsed > 0.0 {
                completed as f64 / elapsed * 60.0
            } else {
                0.0
            };
            let eta = (rate > 0.0)
                .then(|| ((total_archives - completed) as f64 / rate * 60.0).round() as u64);
            self.progress.publish(&job.id,"job_progress",json!({"archive":&relative,"completed":completed,"total":total_archives,"progress_percent":if total_archives>0{completed as f64/total_archives as f64*100.0}else{0.0},"rate_per_minute":rate,"eta_seconds":eta}));
            let immediate = matches!(
                record.result.outcome,
                Outcome::Optimized | Outcome::Corrected | Outcome::Repaired | Outcome::Failed
            ) || !record.issues.is_empty();
            batch.push(record);
            if immediate
                || batch.len() >= self.config.durable_checkpoint_archives
                || last_checkpoint.elapsed() >= self.config.durable_checkpoint
            {
                flush(&self.database, job, &mut batch)?;
                last_checkpoint = std::time::Instant::now();
            }
        }
        flush(&self.database, job, &mut batch)?;
        let status = if failures > 0 {
            JobStatus::Partial
        } else {
            JobStatus::Succeeded
        };
        self.database.finish_job(&job.id, status, None)?;
        self.database.add_event(
            &job.id,
            if failures > 0 { "warning" } else { "success" },
            "job_completed",
            "Job completed",
            None,
            Some(&json!({"status":status})),
        )?;
        self.progress
            .publish(&job.id, "job_completed", json!({"status":status}));
        Ok(())
    }
}

fn prepare_queue(
    database: &Database,
    progress: &ProgressHub,
    job: &Job,
    library: &Library,
) -> Result<Option<PreparedQueue>> {
    let preparation_started = std::time::Instant::now();
    let paths = match scan::discover(database, progress, job, library)? {
        scan::Discovery::Ready(paths) => paths,
        scan::Discovery::Cancelled => return Ok(None),
    };
    let discovery_elapsed = preparation_started.elapsed();
    let mut states = if job.only_path.is_some() {
        let archive = &paths[0];
        database
            .archive_state(&library.id, &archive.relative)?
            .map(|state| HashMap::from([(archive.relative.clone(), state)]))
            .unwrap_or_default()
    } else {
        database.archive_states(&library.id)?
    };
    if job.only_path.is_none() && !matches!(job.policy.scope, crate::domain::Scope::Failed) {
        progress.publish(
            &job.id,
            "reconciling",
            json!({"archives_found":paths.len()}),
        );
        let discovered = paths
            .iter()
            .map(|archive| archive.relative.as_str())
            .collect::<HashSet<_>>();
        if matches!(job.policy.scope, crate::domain::Scope::Library) {
            let reopened = database.reopen_dismissed(&library.id)?;
            if reopened > 0 {
                tracing::info!(job_id=%job.id,reopened,"reopened dismissed issues for full rescan");
            }
        }
        let mut fingerprints = HashMap::<String, Option<String>>::new();
        let mut fingerprint_sizes = HashSet::new();
        let mut fingerprint_without_size = false;
        for (path, state) in &states {
            if !discovered.contains(path.as_str())
                && let Some(fingerprint) = &state.fingerprint
            {
                if let Some(size) = state.current_size {
                    fingerprint_sizes.insert(size);
                } else {
                    fingerprint_without_size = true;
                }
                fingerprints
                    .entry(fingerprint.clone())
                    .and_modify(|path| *path = None)
                    .or_insert_with(|| Some(path.clone()));
            }
        }
        let mut moved = Vec::new();
        if !fingerprints.is_empty() {
            let has_current = fingerprints
                .keys()
                .any(|fingerprint| fingerprint.len() == 64);
            let has_legacy = fingerprints
                .keys()
                .any(|fingerprint| fingerprint.len() == 40);
            for archive in &paths {
                if states.contains_key(&archive.relative) {
                    continue;
                }
                if !fingerprint_without_size
                    && archive
                        .path
                        .metadata()
                        .is_ok_and(|metadata| !fingerprint_sizes.contains(&metadata.len()))
                {
                    continue;
                }
                let matched =
                    archive::fingerprints(&archive.path)
                        .ok()
                        .and_then(|(current, legacy)| {
                            has_current
                                .then(|| fingerprints.get(&current).cloned().flatten())
                                .flatten()
                                .or_else(|| {
                                    has_legacy
                                        .then(|| fingerprints.get(&legacy).cloned().flatten())
                                        .flatten()
                                })
                        });
                if let Some(old_path) = matched
                    && let Some(state) = states.remove(&old_path)
                {
                    fingerprints.retain(|_, path| path.as_deref() != Some(&old_path));
                    moved.push((state.id, archive.relative.clone()));
                    states.insert(archive.relative.clone(), state);
                }
            }
        }
        let missing = states
            .iter()
            .filter(|(path, state)| {
                state.status != "missing" && !discovered.contains(path.as_str())
            })
            .map(|(_, state)| state.id)
            .collect::<Vec<_>>();
        if !database.apply_reconciliation(&job.id, &library.id, &moved, &missing)? {
            return Ok(None);
        }
        tracing::info!(
            job_id = %job.id,
            archives = paths.len(),
            moved = moved.len(),
            missing = missing.len(),
            discovery_ms = discovery_elapsed.as_millis(),
            preparation_ms = preparation_started.elapsed().as_millis(),
            "conversion queue prepared in memory"
        );
    }
    Ok(Some(PreparedQueue {
        archives: paths,
        states,
    }))
}

fn flush(database: &Database, job: &Job, batch: &mut Vec<RecordedArchive>) -> Result<()> {
    database.record_batch(&job.id, &job.library_id, batch)?;
    batch.clear();
    Ok(())
}
