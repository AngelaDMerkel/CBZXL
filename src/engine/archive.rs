use super::{Engine, ImageInfo, MediaRequest};
use crate::{
    db::ArchiveState,
    domain::{
        ArchiveResult, FormatStat, IssueDraft, Job, Library, Outcome, PageResult, RecordedArchive,
        RepairAction, Scope, Strategy,
    },
};
use anyhow::{Context, Result, ensure};
use blake2::{
    Blake2bVar,
    digest::{Update, VariableOutput},
};
use bzip2::read::BzDecoder;
use flate2::read::DeflateDecoder;
use futures::{StreamExt, stream};
use serde_json::json;
use std::{
    collections::{HashMap, HashSet},
    fs::{self, File},
    io::{self, Read, Seek, SeekFrom},
    path::{Component, Path, PathBuf},
    time::{Duration, Instant, SystemTime},
};
use tempfile::{TempDir, TempPath};
use thiserror::Error;
use walkdir::WalkDir;
use zip::{CompressionMethod, ZipArchive, ZipWriter, write::SimpleFileOptions};

const MAX_ENTRIES: usize = 100_000;

#[derive(Debug, Error)]
#[error("{message}")]
pub struct EngineError {
    kind: &'static str,
    stage: &'static str,
    entry: Option<String>,
    related: Option<String>,
    message: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Format {
    Jpeg,
    Png,
    Tiff,
    Bmp,
    Webp,
    Gif,
    Jxl,
    Unknown,
}

impl Format {
    fn convertible(self) -> bool {
        matches!(self, Self::Jpeg | Self::Png | Self::Tiff | Self::Bmp)
    }

    fn label(self) -> &'static str {
        match self {
            Self::Jpeg => "image/jpeg",
            Self::Png => "image/png",
            Self::Tiff => "image/tiff",
            Self::Bmp => "image/bmp",
            Self::Webp => "image/webp",
            Self::Gif => "image/gif",
            Self::Jxl => "image/jxl",
            Self::Unknown => "application/octet-stream",
        }
    }

    fn corrected_path(self, source: &Path) -> Option<PathBuf> {
        let suffix = source.extension()?.to_string_lossy().to_lowercase();
        let (canonical, valid): (&str, &[&str]) = match self {
            Self::Jpeg => ("jpg", &["jpg", "jpeg"]),
            Self::Png => ("png", &["png"]),
            Self::Tiff => ("tiff", &["tif", "tiff"]),
            Self::Bmp => ("bmp", &["bmp"]),
            Self::Webp => ("webp", &["webp"]),
            Self::Gif => ("gif", &["gif"]),
            _ => return None,
        };
        (!valid.contains(&suffix.as_str())).then(|| source.with_extension(canonical))
    }
}

#[derive(Clone)]
struct PageWork {
    path: PathBuf,
    entry: String,
    format: Format,
    force_normalize: bool,
}

struct PageDone {
    result: PageResult,
    issue: Option<IssueDraft>,
    applied: bool,
}

#[derive(Default)]
struct ActionPlan {
    skip: HashSet<String>,
    recover_crc: HashSet<String>,
    preserve: HashSet<String>,
    replacement: Option<(String, PathBuf)>,
    recover_image: Option<String>,
    compatibility: bool,
    repack: bool,
    skip_conversion: bool,
    archive_source: Option<PathBuf>,
    salvage: Option<bool>,
    explicit: bool,
}

impl ActionPlan {
    fn from(action: Option<&RepairAction>, source: &Path) -> Result<Self> {
        let mut plan = Self::default();
        match action {
            None => {}
            Some(RepairAction::Compatibility) => plan.compatibility = true,
            Some(RepairAction::Preserve { entry }) => {
                let entry = safe_name(entry)?.trim_end_matches('/').to_owned();
                plan.recover_crc.insert(entry.clone());
                plan.preserve.insert(entry);
                plan.explicit = true;
            }
            Some(RepairAction::Delete { entry }) => {
                plan.skip
                    .insert(safe_name(entry)?.trim_end_matches('/').to_owned());
                plan.explicit = true;
            }
            Some(RepairAction::Replace { entry, source }) => {
                let entry = safe_name(entry)?.trim_end_matches('/').to_owned();
                plan.skip.insert(entry.clone());
                plan.replacement = Some((entry, source.clone()));
                plan.explicit = true;
            }
            Some(RepairAction::Recover { entry }) => {
                plan.recover_image = Some(safe_name(entry)?.trim_end_matches('/').to_owned());
                plan.explicit = true;
            }
            Some(RepairAction::RebuildCrc { entries }) => {
                for entry in entries {
                    plan.recover_crc
                        .insert(safe_name(entry)?.trim_end_matches('/').to_owned());
                }
                plan.explicit = true;
            }
            Some(RepairAction::Repack) => {
                plan.repack = true;
                plan.explicit = true;
            }
            Some(RepairAction::Restore { reconvert }) => {
                let backup = source.with_extension("cbz.bak");
                ensure!(backup.is_file(), "original backup is unavailable");
                plan.archive_source = Some(backup);
                plan.skip_conversion = !reconvert;
                plan.explicit = true;
            }
            Some(RepairAction::Salvage { partial }) => {
                plan.salvage = Some(*partial);
                plan.explicit = true;
            }
        }
        Ok(plan)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Identity {
    size: u64,
    modified: Option<SystemTime>,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
}

#[cfg(test)]
pub async fn process(
    engine: &Engine,
    job: &Job,
    library: &Library,
    source: &Path,
    relative: &str,
) -> Result<RecordedArchive> {
    let state = engine.database.archive_state(&library.id, relative)?;
    process_mode(engine, job, library, source, relative, state, false).await
}

pub async fn process_known(
    engine: &Engine,
    job: &Job,
    library: &Library,
    source: &Path,
    relative: &str,
    state: Option<ArchiveState>,
) -> Result<RecordedArchive> {
    process_mode(engine, job, library, source, relative, state, false).await
}

pub async fn process_compatibility_known(
    engine: &Engine,
    job: &Job,
    library: &Library,
    source: &Path,
    relative: &str,
    state: Option<ArchiveState>,
) -> Result<RecordedArchive> {
    process_mode(engine, job, library, source, relative, state, true).await
}

async fn process_mode(
    engine: &Engine,
    job: &Job,
    library: &Library,
    source: &Path,
    relative: &str,
    state: Option<ArchiveState>,
    automatic_compatibility: bool,
) -> Result<RecordedArchive> {
    let started = Instant::now();
    let source_identity = identity(source)?;
    if !stable(&source_identity, engine.config.stable_for) {
        return Ok(record(
            relative,
            Outcome::Deferred,
            (source_identity.size, source_identity.size),
            started,
            (None, None),
            Vec::new(),
            Vec::new(),
        ));
    }
    let mtime_ns = source_identity.modified.and_then(modified_ns);
    if matches!(job.policy.scope, Scope::Unprocessed)
        && job.action.is_none()
        && !fingerprint_due(relative, engine.config.fingerprint_audit_days)
        && state.as_ref().is_some_and(|state| {
            state.status == "processed"
                && state.current_size == Some(source_identity.size)
                && mtime_matches(state.mtime_ns, mtime_ns, state.fingerprint.as_deref())
        })
    {
        return Ok(RecordedArchive {
            refresh_identity: state
                .as_ref()
                .is_some_and(|state| state.mtime_ns != mtime_ns),
            mtime_ns,
            input_fingerprint: state.as_ref().and_then(|state| state.fingerprint.clone()),
            final_fingerprint: state.as_ref().and_then(|state| state.fingerprint.clone()),
            ..record(
                relative,
                Outcome::AlreadyCurrent,
                (source_identity.size, source_identity.size),
                started,
                (None, None),
                Vec::new(),
                Vec::new(),
            )
        });
    }

    let mut plan = ActionPlan::from(job.action.as_ref(), source)?;
    if automatic_compatibility {
        plan.compatibility = true;
        plan.explicit = false;
    }
    let (input_fingerprint, input_legacy_fingerprint) = guard_fingerprints(source)?;
    if matches!(job.policy.scope, Scope::Unprocessed)
        && job.action.is_none()
        && state.as_ref().is_some_and(|state| {
            state.fingerprint.as_deref().is_some_and(|expected| {
                fingerprint_matches(
                    expected,
                    &input_fingerprint,
                    input_legacy_fingerprint.as_deref(),
                )
            })
        })
    {
        return Ok(RecordedArchive {
            refresh_identity: state.as_ref().is_some_and(|state| {
                state.mtime_ns != mtime_ns
                    || state.fingerprint.as_deref() != Some(input_fingerprint.as_str())
            }),
            mtime_ns,
            input_fingerprint: Some(input_fingerprint.clone()),
            final_fingerprint: Some(input_fingerprint),
            ..record(
                relative,
                Outcome::AlreadyCurrent,
                (source_identity.size, source_identity.size),
                started,
                (None, None),
                Vec::new(),
                Vec::new(),
            )
        });
    }
    let central_inventory = inventory(source)?;
    let has_conversion_candidate = central_inventory
        .iter()
        .any(|format| matches!(format.format.as_str(), "JPEG" | "PNG" | "TIFF" | "BMP"));
    if job.action.is_none()
        && matches!(
            job.policy.strategy,
            Strategy::Lossless | Strategy::Smart | Strategy::Lossy
        )
        && !has_conversion_candidate
    {
        if state
            .as_ref()
            .is_some_and(|state| state.current_size == Some(source_identity.size))
        {
            return Ok(RecordedArchive {
                refresh_identity: true,
                mtime_ns,
                input_fingerprint: Some(input_fingerprint.clone()),
                final_fingerprint: Some(input_fingerprint),
                formats: central_inventory,
                ..record(
                    relative,
                    Outcome::AlreadyCurrent,
                    (source_identity.size, source_identity.size),
                    started,
                    (None, None),
                    Vec::new(),
                    Vec::new(),
                )
            });
        }
        let pages = central_inventory
            .iter()
            .map(|format| format.files as usize)
            .sum();
        return Ok(RecordedArchive {
            result: ArchiveResult {
                path: relative.into(),
                outcome: Outcome::Retained,
                source_size: source_identity.size,
                final_size: source_identity.size,
                pages,
                converted: 0,
                retained: pages,
                failed: 0,
                duration_ms: started.elapsed().as_millis() as u64,
                fingerprint: Some(input_fingerprint.clone()),
                error: None,
            },
            refresh_identity: false,
            mtime_ns,
            input_fingerprint: Some(input_fingerprint.clone()),
            final_fingerprint: Some(input_fingerprint),
            mode: Some("No eligible image formats".into()),
            effort: None,
            pages: Vec::new(),
            issues: Vec::new(),
            formats: central_inventory,
        });
    }
    let archive_source = plan.archive_source.as_deref().unwrap_or(source);
    let workspace = workspace(&engine.config.scratch)?;
    if let Some(partial) = plan.salvage {
        extract_salvage(
            archive_source,
            workspace.path(),
            engine.config.archive_limit,
            partial,
        )?;
    } else {
        extract(
            archive_source,
            workspace.path(),
            engine.config.archive_limit,
            &plan,
        )?;
    }
    apply_action(engine, workspace.path(), &plan).await?;
    let files = files(workspace.path())?;
    let image_files: Vec<_> = files
        .iter()
        .filter_map(|path| {
            let format = detect(path).ok()?;
            matches!(
                format,
                Format::Jpeg
                    | Format::Png
                    | Format::Tiff
                    | Format::Bmp
                    | Format::Webp
                    | Format::Gif
                    | Format::Jxl
            )
            .then(|| (path.clone(), format))
        })
        .collect();
    if plan.salvage.is_some() {
        verify_recovered_images(engine, &image_files).await?;
    }

    if matches!(job.policy.strategy, Strategy::Audit | Strategy::Recheck) {
        verify_extracted(
            engine,
            &image_files,
            matches!(job.policy.strategy, Strategy::Audit),
        )
        .await?;
        let outcome = if matches!(job.policy.strategy, Strategy::Audit) {
            Outcome::Audited
        } else {
            Outcome::Retained
        };
        return Ok(RecordedArchive {
            result: ArchiveResult {
                path: relative.into(),
                outcome,
                source_size: source_identity.size,
                final_size: source_identity.size,
                pages: image_files.len(),
                converted: 0,
                retained: image_files.len(),
                failed: 0,
                duration_ms: started.elapsed().as_millis() as u64,
                fingerprint: Some(input_fingerprint.clone()),
                error: None,
            },
            refresh_identity: false,
            mtime_ns,
            input_fingerprint: Some(input_fingerprint.clone()),
            final_fingerprint: Some(input_fingerprint),
            mode: Some(
                if matches!(job.policy.strategy, Strategy::Audit) {
                    "Integrity audit"
                } else {
                    "Recheck"
                }
                .into(),
            ),
            effort: Some(job.policy.effort),
            pages: Vec::new(),
            issues: Vec::new(),
            formats: Vec::new(),
        });
    }

    let mut work = conversion_work(workspace.path(), &image_files)?;
    work.retain(|page| !plan.preserve.contains(&page.entry));
    if plan.repack || plan.skip_conversion {
        work.clear();
    }
    validate_outputs(&work, &files, workspace.path())?;
    let mut policy = job.policy.clone();
    if plan.compatibility {
        policy.effort = policy.effort.min(7);
        policy.threads = 1;
    }
    for page in &mut work {
        page.force_normalize = plan.compatibility;
    }
    let (workers, encoder_threads) = parallelism(policy.threads, work.len());
    let media = engine.media.clone();
    let page_total = work.len();
    let page_completed = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut completed: Vec<PageDone> = stream::iter(work)
        .map(|page| {
            let media = media.clone();
            let policy = policy.clone();
            let fallback = page.clone();
            let progress = engine.progress.clone();
            let job_id = job.id.clone();
            let archive = relative.to_owned();
            let page_completed = page_completed.clone();
            async move {
                let result = convert_page(media, policy, page, encoder_threads)
                    .await
                    .unwrap_or_else(|error| page_failure(fallback, error));
                let completed = page_completed.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                progress.publish(&job_id,"page_completed",json!({"archive":archive,"page":completed,"total_pages":page_total,"entry":result.result.entry_path,"outcome":result.result.outcome}));
                result
            }
        })
        .buffer_unordered(workers)
        .collect::<Vec<_>>()
        .await;
    completed.extend(plan.preserve.iter().map(|entry| PageDone {
        result: PageResult {
            entry_path: entry.clone(),
            source_format: None,
            source_size: None,
            candidate_size: None,
            final_size: None,
            outcome: "preserved".into(),
            mode: Some("Preserved after repair".into()),
            distance: None,
            effort: None,
            reason: Some("Selected entry was recovered without conversion".into()),
            attempts: Vec::new(),
        },
        issue: None,
        applied: false,
    }));

    let applied = completed.iter().filter(|page| page.applied).count();
    let issues: Vec<_> = completed
        .iter()
        .filter_map(|page| page.issue.clone())
        .collect();
    let mut page_results: Vec<_> = completed.into_iter().map(|page| page.result).collect();
    let converted_count = page_results
        .iter()
        .filter(|page| page.outcome == "converted")
        .count();
    let corrected_count = page_results
        .iter()
        .filter(|page| page.outcome == "extension_corrected")
        .count();
    if applied == 0 && !plan.explicit {
        return Ok(RecordedArchive {
            result: ArchiveResult {
                path: relative.into(),
                outcome: if issues.is_empty() {
                    Outcome::Retained
                } else {
                    Outcome::Failed
                },
                source_size: source_identity.size,
                final_size: source_identity.size,
                pages: image_files.len(),
                converted: 0,
                retained: image_files.len(),
                failed: issues.len(),
                duration_ms: started.elapsed().as_millis() as u64,
                fingerprint: Some(input_fingerprint.clone()),
                error: issues.first().map(|issue| issue.message.clone()),
            },
            refresh_identity: false,
            mtime_ns,
            input_fingerprint: Some(input_fingerprint.clone()),
            final_fingerprint: Some(input_fingerprint),
            mode: Some("Original retained".into()),
            effort: Some(job.policy.effort),
            pages: page_results,
            issues,
            formats: Vec::new(),
        });
    }

    let candidate = candidate_path(source, &job.id);
    rebuild(
        &candidate,
        archive_source,
        workspace.path(),
        !plan.repack && plan.salvage.is_none(),
        &plan,
    )?;
    verify_candidate(engine, &candidate, workspace.path()).await?;
    let candidate_size = candidate.metadata()?.len();
    let correction_only = corrected_count > 0 && converted_count == 0;
    let allowed_growth = extension_overhead(&page_results);
    if candidate_size >= source_identity.size
        && (!plan.explicit || plan.repack)
        && !(correction_only && candidate_size <= source_identity.size + allowed_growth)
    {
        let _ = fs::remove_file(&candidate);
        for page in &mut page_results {
            if page.outcome == "converted" {
                page.outcome = "retained".into();
                page.final_size = page.source_size;
                page.reason = Some("complete replacement archive was not smaller".into());
            }
        }
        return Ok(RecordedArchive {
            result: ArchiveResult {
                path: relative.into(),
                outcome: if issues.is_empty() {
                    Outcome::Retained
                } else {
                    Outcome::Failed
                },
                source_size: source_identity.size,
                final_size: source_identity.size,
                pages: image_files.len(),
                converted: 0,
                retained: image_files.len(),
                failed: issues.len(),
                duration_ms: started.elapsed().as_millis() as u64,
                fingerprint: Some(input_fingerprint.clone()),
                error: issues.first().map(|issue| issue.message.clone()),
            },
            refresh_identity: false,
            mtime_ns,
            input_fingerprint: Some(input_fingerprint.clone()),
            final_fingerprint: Some(input_fingerprint),
            mode: Some("Original retained".into()),
            effort: Some(job.policy.effort),
            pages: page_results,
            issues,
            formats: Vec::new(),
        });
    }

    let candidate_fingerprint = fingerprint(&candidate)?;
    let mut completed = RecordedArchive {
        result: ArchiveResult {
            path: relative.into(),
            outcome: if plan.explicit {
                Outcome::Repaired
            } else if correction_only {
                Outcome::Corrected
            } else {
                Outcome::Optimized
            },
            source_size: source_identity.size,
            final_size: candidate_size,
            pages: image_files.len(),
            converted: converted_count,
            retained: image_files.len().saturating_sub(converted_count),
            failed: issues.len(),
            duration_ms: started.elapsed().as_millis() as u64,
            fingerprint: Some(candidate_fingerprint.clone()),
            error: issues.first().map(|issue| issue.message.clone()),
        },
        refresh_identity: false,
        mtime_ns: None,
        input_fingerprint: Some(input_fingerprint.clone()),
        final_fingerprint: Some(candidate_fingerprint.clone()),
        mode: Some(
            if plan.explicit {
                "Repair"
            } else if correction_only {
                "Extension correction"
            } else {
                strategy_name(job.policy.strategy)
            }
            .into(),
        ),
        effort: Some(job.policy.effort),
        pages: page_results,
        issues,
        formats: Vec::new(),
    };
    engine.database.put_commit_intent(
        &job.id,
        &library.id,
        relative,
        &input_fingerprint,
        &candidate_fingerprint,
        &json!({"record":&completed}),
    )?;
    require_unchanged(source, &source_identity, &input_fingerprint)?;
    if job.policy.backup {
        let backup = source.with_extension("cbz.bak");
        if !backup.exists() {
            let temporary = tempfile::Builder::new()
                .prefix(".cbzxl-backup-")
                .tempfile_in(source.parent().context("archive has no parent")?)?;
            fs::copy(source, temporary.path())?;
            File::open(temporary.path())?.sync_all()?;
            ensure!(
                guard_fingerprint(temporary.path())? == input_fingerprint,
                "original backup failed verification"
            );
            temporary
                .persist_noclobber(&backup)
                .map_err(|error| error.error)?;
            sync_parent(&backup)?;
        }
    }
    File::open(&candidate)?.sync_all()?;
    fs::rename(&candidate, source)?;
    sync_parent(source)?;
    if let Some((_, staged)) = &plan.replacement {
        let _ = fs::remove_file(staged);
    }
    completed.mtime_ns = identity(source)?.modified.and_then(modified_ns);
    Ok(completed)
}

pub fn failure(path: &Path, relative: String, error: anyhow::Error) -> RecordedArchive {
    let size = path.metadata().map(|value| value.len()).unwrap_or(0);
    let (kind, stage, entry, related) = error
        .downcast_ref::<EngineError>()
        .map(|error| {
            (
                error.kind,
                error.stage,
                error.entry.clone(),
                error.related.clone(),
            )
        })
        .unwrap_or(("archive_failed", "processing", None, None));
    let message = error.to_string();
    let issue = IssueDraft {
        kind: kind.into(),
        stage: stage.into(),
        entry_path: entry,
        related_entry_path: related,
        message: message.clone(),
        detail: Some(format!("{error:#}")),
    };
    RecordedArchive {
        result: ArchiveResult {
            path: relative,
            outcome: Outcome::Failed,
            source_size: size,
            final_size: size,
            pages: 0,
            converted: 0,
            retained: 0,
            failed: 1,
            duration_ms: 0,
            fingerprint: None,
            error: Some(message),
        },
        refresh_identity: false,
        mtime_ns: path
            .metadata()
            .ok()
            .and_then(|value| value.modified().ok())
            .and_then(modified_ns),
        input_fingerprint: None,
        final_fingerprint: None,
        mode: None,
        effort: None,
        pages: Vec::new(),
        issues: vec![issue],
        formats: Vec::new(),
    }
}

pub(super) fn retryable(error: &anyhow::Error) -> bool {
    if let Some(error) = error.downcast_ref::<EngineError>()
        && matches!(
            error.kind,
            "unsafe_archive_path"
                | "filename_collision"
                | "archive_corrupt"
                | "crc_mismatch"
                | "source_changed"
        )
    {
        return false;
    }
    let value = error.to_string().to_lowercase();
    ![
        "unsafe",
        "symlink",
        "duplicate archive path",
        "source archive changed",
        "permission denied",
        "no such file",
        "truncated",
        "crc",
    ]
    .iter()
    .any(|token| value.contains(token))
        && [
            "encode",
            "decode",
            "verification",
            "timed out",
            "timeout",
            "resource",
            "memory",
            "temporar",
            "input/output",
        ]
        .iter()
        .any(|token| value.contains(token))
}

fn record(
    path: &str,
    outcome: Outcome,
    sizes: (u64, u64),
    started: Instant,
    fingerprints: (Option<String>, Option<String>),
    pages: Vec<PageResult>,
    issues: Vec<IssueDraft>,
) -> RecordedArchive {
    let (source_size, final_size) = sizes;
    let (input_fingerprint, final_fingerprint) = fingerprints;
    RecordedArchive {
        result: ArchiveResult {
            path: path.into(),
            outcome,
            source_size,
            final_size,
            pages: pages.len(),
            converted: pages
                .iter()
                .filter(|page| page.outcome == "converted")
                .count(),
            retained: pages
                .iter()
                .filter(|page| page.outcome == "retained")
                .count(),
            failed: issues.len(),
            duration_ms: started.elapsed().as_millis() as u64,
            fingerprint: final_fingerprint.clone(),
            error: issues.first().map(|issue| issue.message.clone()),
        },
        refresh_identity: false,
        mtime_ns: None,
        input_fingerprint,
        final_fingerprint,
        mode: None,
        effort: None,
        pages,
        issues,
        formats: Vec::new(),
    }
}

async fn apply_action(engine: &Engine, root: &Path, plan: &ActionPlan) -> Result<()> {
    if let Some((entry, source)) = &plan.replacement {
        ensure!(source.is_file(), "staged replacement is unavailable");
        let target = entry_target(root, entry)?;
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::copy(source, &target)?;
        engine
            .media
            .info(&target)
            .await
            .context("replacement image is unreadable")?;
    }
    if let Some(entry) = &plan.recover_image {
        let target = entry_target(root, entry)?;
        ensure!(
            target.is_file(),
            "image selected for recovery is unavailable"
        );
        let recovered = tempfile::Builder::new()
            .suffix(".png")
            .tempfile_in(target.parent().unwrap())?;
        engine.media.normalize(&target, recovered.path()).await?;
        engine
            .media
            .info(recovered.path())
            .await
            .context("recovered image is unreadable")?;
        fs::rename(recovered.path(), target)?;
    }
    Ok(())
}

fn entry_target(root: &Path, entry: &str) -> Result<PathBuf> {
    let entry = safe_name(entry)?;
    let target = root.join(entry.trim_end_matches('/'));
    ensure!(
        target.starts_with(root),
        "repair entry escapes the workspace"
    );
    Ok(target)
}

async fn convert_page(
    media: std::sync::Arc<dyn super::MediaBackend>,
    policy: crate::domain::Policy,
    page: PageWork,
    encoder_threads: usize,
) -> Result<PageDone> {
    let source_size = page.path.metadata()?.len();
    if !page.format.convertible() {
        return correct_or_retain(page, source_size, Vec::new(), None, policy.effort);
    }
    let info = media.info(&page.path).await?;
    let attempts = distances(&policy, source_size, &info);
    let normalize = page.force_normalize
        || !matches!(page.format, Format::Jpeg | Format::Png)
        || info.colorspace.to_uppercase().contains("CMYK");
    let normalized = normalize
        .then(|| {
            tempfile::Builder::new()
                .suffix(".png")
                .tempfile_in(page.path.parent().unwrap())
        })
        .transpose()?;
    let input = if let Some(file) = &normalized {
        media.normalize(&page.path, file.path()).await?;
        file.path()
    } else {
        &page.path
    };
    let mut records = Vec::new();
    let mut smallest = None;
    let mut last_verification = None;
    let mut source_signature = None;
    for (distance, mode) in attempts {
        let mut fallback_normalized = None;
        let mut normalized_retry_added = normalize;
        let mut compatibility_retry_added = false;
        let mut strategies = vec![(input.to_owned(), encoder_threads, policy.effort, "standard")];
        let mut index = 0;
        while index < strategies.len() {
            let (strategy_input, threads, effort, strategy) = strategies[index].clone();
            index += 1;
            let candidate = temporary_candidate(&page.path)?;
            let encoded = media
                .encode(MediaRequest {
                    source: strategy_input.clone(),
                    destination: candidate.to_path_buf(),
                    effort,
                    distance,
                    threads,
                })
                .await;
            let encoded = match encoded {
                Ok(value) => value,
                Err(error) => {
                    records.push(json!({"mode":mode,"distance":distance,"effort":effort,"strategy":strategy,"error":error.to_string()}));
                    if !normalized_retry_added {
                        normalized_retry_added = true;
                        let file = tempfile::Builder::new()
                            .suffix(".png")
                            .tempfile_in(page.path.parent().unwrap())?;
                        if media.normalize(&page.path, file.path()).await.is_ok() {
                            strategies.push((
                                file.path().to_owned(),
                                threads,
                                effort,
                                "normalized input",
                            ));
                            fallback_normalized = Some(file);
                        }
                    }
                    if !compatibility_retry_added && (threads > 1 || effort > 7) {
                        compatibility_retry_added = true;
                        let compatibility_input = fallback_normalized
                            .as_ref()
                            .map_or_else(|| strategy_input.clone(), |file| file.path().to_owned());
                        strategies.push((
                            compatibility_input,
                            1,
                            effort.min(7),
                            "single-thread compatibility",
                        ));
                    }
                    continue;
                }
            };
            smallest = Some(smallest.map_or(encoded.size, |value: u64| value.min(encoded.size)));
            if encoded.size >= source_size {
                records.push(json!({"mode":mode,"distance":distance,"effort":effort,"strategy":strategy,"candidate_size":encoded.size,"reason":"not_smaller"}));
                continue;
            }
            if distance == 0.0 && source_signature.is_none() {
                source_signature = Some(media.signature(&page.path).await?);
            }
            if let Err(error) = media
                .verify(
                    &page.path,
                    &encoded.path,
                    distance == 0.0,
                    page.format == Format::Jpeg && strategy_input == page.path,
                    source_signature.as_deref(),
                )
                .await
            {
                last_verification = Some((mode, distance, effort, encoded.size, error.to_string()));
                records.push(json!({"mode":mode,"distance":distance,"effort":effort,"strategy":strategy,"candidate_size":encoded.size,"reason":"verification_failed","error":error.to_string()}));
                if !normalized_retry_added {
                    normalized_retry_added = true;
                    let file = tempfile::Builder::new()
                        .suffix(".png")
                        .tempfile_in(page.path.parent().unwrap())?;
                    if media.normalize(&page.path, file.path()).await.is_ok() {
                        strategies.push((
                            file.path().to_owned(),
                            threads,
                            effort,
                            "normalized input",
                        ));
                        fallback_normalized = Some(file);
                    }
                }
                if !compatibility_retry_added && (threads > 1 || effort > 7) {
                    compatibility_retry_added = true;
                    let compatibility_input = fallback_normalized
                        .as_ref()
                        .map_or_else(|| strategy_input.clone(), |file| file.path().to_owned());
                    strategies.push((
                        compatibility_input,
                        1,
                        effort.min(7),
                        "single-thread compatibility",
                    ));
                }
                continue;
            }
            let final_path = page.path.with_extension("jxl");
            fs::rename(&encoded.path, &final_path)?;
            fs::remove_file(&page.path)?;
            records.push(json!({"mode":mode,"distance":distance,"effort":effort,"strategy":strategy,"candidate_size":encoded.size,"reason":"verified"}));
            return Ok(PageDone {
                result: PageResult {
                    entry_path: page.entry,
                    source_format: Some(page.format.label().into()),
                    source_size: Some(source_size),
                    candidate_size: Some(encoded.size),
                    final_size: Some(encoded.size),
                    outcome: "converted".into(),
                    mode: Some(mode.into()),
                    distance: Some(distance),
                    effort: Some(effort),
                    reason: Some(format!(
                        "verified candidate saved {} bytes",
                        source_size - encoded.size
                    )),
                    attempts: records,
                },
                issue: None,
                applied: true,
            });
        }
    }
    if let Some((mode, distance, effort, candidate_size, error)) = last_verification {
        return Ok(PageDone {
            result: PageResult {
                entry_path: page.entry.clone(),
                source_format: Some(page.format.label().into()),
                source_size: Some(source_size),
                candidate_size: Some(candidate_size),
                final_size: Some(source_size),
                outcome: "failed".into(),
                mode: Some(mode.into()),
                distance: Some(distance),
                effort: Some(effort),
                reason: Some(error.clone()),
                attempts: records,
            },
            issue: Some(IssueDraft {
                kind: "page_verification_failed".into(),
                stage: "verifying".into(),
                entry_path: Some(page.entry),
                related_entry_path: None,
                message: "A smaller JPEG XL candidate failed verification".into(),
                detail: Some(error),
            }),
            applied: false,
        });
    }
    correct_or_retain(page, source_size, records, smallest, policy.effort)
}

fn correct_or_retain(
    page: PageWork,
    source_size: u64,
    records: Vec<serde_json::Value>,
    smallest: Option<u64>,
    effort: u8,
) -> Result<PageDone> {
    if let Some(corrected) = page.format.corrected_path(&page.path) {
        if corrected.exists() {
            return Ok(PageDone {
                result: PageResult {
                    entry_path: page.entry.clone(),
                    source_format: Some(page.format.label().into()),
                    source_size: Some(source_size),
                    candidate_size: smallest,
                    final_size: Some(source_size),
                    outcome: "failed".into(),
                    mode: Some("Extension correction".into()),
                    distance: None,
                    effort: Some(effort),
                    reason: Some("the corrected filename already exists".into()),
                    attempts: records,
                },
                issue: Some(IssueDraft {
                    kind: "filename_collision".into(),
                    stage: "preparing".into(),
                    entry_path: Some(page.entry.clone()),
                    related_entry_path: corrected
                        .file_name()
                        .map(|value| value.to_string_lossy().into_owned()),
                    message: "The corrected image filename would overwrite another entry".into(),
                    detail: None,
                }),
                applied: false,
            });
        }
        fs::rename(&page.path, &corrected)?;
        return Ok(PageDone {
            result: PageResult {
                entry_path: page.entry,
                source_format: Some(page.format.label().into()),
                source_size: Some(source_size),
                candidate_size: smallest,
                final_size: Some(source_size),
                outcome: "extension_corrected".into(),
                mode: Some("Extension correction".into()),
                distance: None,
                effort: Some(effort),
                reason: Some("filename extension corrected to match detected content".into()),
                attempts: records,
            },
            issue: None,
            applied: true,
        });
    }
    Ok(PageDone {
        result: PageResult {
            entry_path: page.entry,
            source_format: Some(page.format.label().into()),
            source_size: Some(source_size),
            candidate_size: smallest,
            final_size: Some(source_size),
            outcome: "retained".into(),
            mode: Some("Original retained".into()),
            distance: None,
            effort: Some(effort),
            reason: Some("no JPEG XL candidate was smaller".into()),
            attempts: records,
        },
        issue: None,
        applied: false,
    })
}

fn page_failure(page: PageWork, error: anyhow::Error) -> PageDone {
    PageDone {
        result: PageResult {
            entry_path: page.entry.clone(),
            source_format: Some(page.format.label().into()),
            source_size: page.path.metadata().ok().map(|value| value.len()),
            candidate_size: None,
            final_size: None,
            outcome: "failed".into(),
            mode: None,
            distance: None,
            effort: None,
            reason: Some(error.to_string()),
            attempts: Vec::new(),
        },
        issue: Some(IssueDraft {
            kind: "page_conversion_error".into(),
            stage: "encoding".into(),
            entry_path: Some(page.entry),
            related_entry_path: None,
            message: error.to_string(),
            detail: Some(format!("{error:#}")),
        }),
        applied: false,
    }
}

fn conversion_work(root: &Path, images: &[(PathBuf, Format)]) -> Result<Vec<PageWork>> {
    images
        .iter()
        .filter(|(path, format)| format.convertible() || format.corrected_path(path).is_some())
        .map(|(path, format)| {
            Ok(PageWork {
                path: path.clone(),
                entry: path
                    .strip_prefix(root)?
                    .to_string_lossy()
                    .replace('\\', "/"),
                format: *format,
                force_normalize: false,
            })
        })
        .collect::<Result<_>>()
}

fn validate_outputs(work: &[PageWork], files: &[PathBuf], root: &Path) -> Result<()> {
    let sources: HashSet<_> = work
        .iter()
        .map(|page| page.path.to_string_lossy().to_lowercase())
        .collect();
    let occupied: HashMap<_, _> = files
        .iter()
        .filter(|path| !sources.contains(&path.to_string_lossy().to_lowercase()))
        .map(|path| (path.to_string_lossy().to_lowercase(), path))
        .collect();
    let mut outputs: HashMap<String, &PageWork> = HashMap::new();
    for page in work {
        let output = page
            .path
            .with_extension("jxl")
            .to_string_lossy()
            .to_lowercase();
        if let Some(existing) = occupied.get(&output) {
            return Err(EngineError {
                kind: "filename_collision",
                stage: "preparing",
                entry: Some(page.entry.clone()),
                related: Some(
                    existing
                        .strip_prefix(root)?
                        .to_string_lossy()
                        .replace('\\', "/"),
                ),
                message: format!("{} cannot become JXL because that path exists", page.entry),
            }
            .into());
        }
        if let Some(existing) = outputs.insert(output, page) {
            return Err(EngineError {
                kind: "filename_collision",
                stage: "preparing",
                entry: Some(page.entry.clone()),
                related: Some(existing.entry.clone()),
                message: "two images require the same JXL path".into(),
            }
            .into());
        }
    }
    Ok(())
}

fn extract(source: &Path, destination: &Path, limit: u64, plan: &ActionPlan) -> Result<()> {
    let mut archive = ZipArchive::new(File::open(source)?)?;
    ensure!(archive.len() <= MAX_ENTRIES, "archive has too many entries");
    let mut total = 0_u64;
    let mut names = HashSet::new();
    for index in 0..archive.len() {
        let mut entry = archive.by_index(index)?;
        let name = safe_name(entry.name())?;
        let key = name.to_lowercase();
        ensure!(names.insert(key), "duplicate archive path: {name}");
        ensure!(
            !is_symlink(entry.unix_mode()),
            "symlinks are not allowed: {name}"
        );
        if plan.skip.contains(name.trim_end_matches('/')) {
            continue;
        }
        total = total
            .checked_add(entry.size())
            .context("archive size overflow")?;
        ensure!(total <= limit, "archive expands past the configured limit");
        let target = destination.join(Path::new(&name));
        if entry.is_dir() {
            fs::create_dir_all(&target)?;
        } else if plan.recover_crc.contains(&name) {
            let payload = recover_entry(source, &entry, limit)?;
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(&target, payload)?;
        } else {
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)?;
            }
            io::copy(&mut entry, &mut File::create(&target)?)?;
        }
    }
    Ok(())
}

fn recover_entry<R: Read>(
    source: &Path,
    entry: &zip::read::ZipFile<'_, R>,
    limit: u64,
) -> Result<Vec<u8>> {
    ensure!(
        entry.size() <= limit,
        "recovered entry exceeds the archive limit"
    );
    let mut source = File::open(source)?;
    source.seek(SeekFrom::Start(entry.data_start()))?;
    let mut compressed = vec![0_u8; entry.compressed_size() as usize];
    source.read_exact(&mut compressed)?;
    let payload = decompress_payload(compression_code(entry.compression()), &compressed, limit)
        .with_context(|| format!("ZIP entry cannot be recovered: {}", entry.name()))?;
    ensure!(
        payload.len() as u64 <= limit,
        "recovered entry exceeds the archive limit"
    );
    Ok(payload)
}

fn decompress_payload(method: u16, compressed: &[u8], limit: u64) -> Result<Vec<u8>> {
    if method == 0 {
        return Ok(compressed.to_vec());
    }
    let mut output = Vec::new();
    match method {
        8 => DeflateDecoder::new(compressed)
            .take(limit + 1)
            .read_to_end(&mut output)?,
        12 => BzDecoder::new(compressed)
            .take(limit + 1)
            .read_to_end(&mut output)?,
        14 => {
            let stream = liblzma::stream::Stream::new_lzma_decoder(u64::MAX)?;
            liblzma::read::XzDecoder::new_stream(compressed, stream)
                .take(limit + 1)
                .read_to_end(&mut output)?
        }
        _ => anyhow::bail!("ZIP compression method {method} is not recoverable"),
    };
    ensure!(
        output.len() as u64 <= limit,
        "recovered entry exceeds the archive limit"
    );
    Ok(output)
}

fn zip64_sizes(extra: &[u8], mut compressed: u64, mut uncompressed: u64) -> Result<(u64, u64)> {
    if compressed != u32::MAX as u64 && uncompressed != u32::MAX as u64 {
        return Ok((compressed, uncompressed));
    }
    let mut offset = 0;
    while offset + 4 <= extra.len() {
        let kind = u16::from_le_bytes(extra[offset..offset + 2].try_into().unwrap());
        let size = u16::from_le_bytes(extra[offset + 2..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        ensure!(offset + size <= extra.len(), "truncated ZIP extra field");
        if kind == 1 {
            let payload = &extra[offset..offset + size];
            let mut cursor = 0;
            if uncompressed == u32::MAX as u64 && cursor + 8 <= payload.len() {
                uncompressed = u64::from_le_bytes(payload[cursor..cursor + 8].try_into().unwrap());
                cursor += 8;
            }
            if compressed == u32::MAX as u64 && cursor + 8 <= payload.len() {
                compressed = u64::from_le_bytes(payload[cursor..cursor + 8].try_into().unwrap());
            }
            break;
        }
        offset += size;
    }
    ensure!(
        compressed != u32::MAX as u64 && uncompressed != u32::MAX as u64,
        "ZIP64 sizes are incomplete"
    );
    Ok((compressed, uncompressed))
}

pub(super) fn entry_bytes(source: &Path, entry_name: &str, limit: u64) -> Result<Vec<u8>> {
    let entry_name = safe_name(entry_name)?;
    let mut archive = ZipArchive::new(File::open(source)?)?;
    ensure!(archive.len() <= MAX_ENTRIES, "archive has too many entries");
    let mut entry = archive.by_name(&entry_name)?;
    ensure!(
        entry.size() <= limit,
        "archive entry exceeds the preview limit"
    );
    let mut payload = Vec::with_capacity(entry.size() as usize);
    if entry.read_to_end(&mut payload).is_err() {
        payload = recover_entry(source, &entry, limit)?;
    }
    ensure!(
        payload.len() as u64 <= limit,
        "archive entry exceeds the preview limit"
    );
    Ok(payload)
}

pub(super) fn extract_salvage(
    source: &Path,
    destination: &Path,
    limit: u64,
    partial: bool,
) -> Result<()> {
    const LOCAL: u32 = 0x0403_4b50;
    const CENTRAL: [u32; 4] = [0x0201_4b50, 0x0605_4b50, 0x0606_4b50, 0x0706_4b50];
    let mut file = File::open(source)?;
    let archive_size = file.metadata()?.len();
    let mut offset = 0_u64;
    let mut recovered = 0_usize;
    let mut total = 0_u64;
    let mut names = HashSet::new();
    while offset + 4 <= archive_size {
        file.seek(SeekFrom::Start(offset))?;
        let mut header = [0_u8; 30];
        if file.read_exact(&mut header[..4]).is_err() {
            break;
        }
        let signature = u32::from_le_bytes(header[..4].try_into().unwrap());
        if CENTRAL.contains(&signature) {
            break;
        }
        if signature != LOCAL || file.read_exact(&mut header[4..]).is_err() {
            if partial && recovered > 0 {
                break;
            }
            return Err(EngineError {
                kind: "archive_corrupt",
                stage: "recovering",
                entry: None,
                related: None,
                message: format!("invalid local ZIP header at byte {offset}"),
            }
            .into());
        }
        let flags = u16::from_le_bytes(header[6..8].try_into().unwrap());
        let method = u16::from_le_bytes(header[8..10].try_into().unwrap());
        let expected_crc = u32::from_le_bytes(header[14..18].try_into().unwrap());
        let compressed_size = u32::from_le_bytes(header[18..22].try_into().unwrap()) as u64;
        let declared_size = u32::from_le_bytes(header[22..26].try_into().unwrap()) as u64;
        let name_length = u16::from_le_bytes(header[26..28].try_into().unwrap()) as usize;
        let extra_length = u16::from_le_bytes(header[28..30].try_into().unwrap()) as usize;
        ensure!(flags & 1 == 0, "encrypted ZIP entries cannot be salvaged");
        ensure!(
            flags & 8 == 0,
            "ZIP entries with data descriptors require external repair"
        );
        let mut raw_name = vec![0_u8; name_length];
        file.read_exact(&mut raw_name)?;
        let mut extra = vec![0_u8; extra_length];
        file.read_exact(&mut extra)?;
        let decoded_name = if flags & 0x800 != 0 {
            std::str::from_utf8(&raw_name)?.to_owned()
        } else {
            oem_cp::decode_string_complete_table(
                &raw_name,
                &oem_cp::code_table::DECODING_TABLE_CP437,
            )
        };
        let name = safe_name(&decoded_name)?;
        let (compressed_size, declared_size) = zip64_sizes(&extra, compressed_size, declared_size)?;
        ensure!(
            names.insert(name.to_lowercase()),
            "duplicate archive path: {name}"
        );
        let data_offset = offset + 30 + name_length as u64 + extra_length as u64;
        let data_end = data_offset.saturating_add(compressed_size);
        if data_end > archive_size {
            if partial && recovered > 0 {
                break;
            }
            return Err(EngineError {
                kind: "archive_corrupt",
                stage: "recovering",
                entry: Some(name),
                related: None,
                message: "compressed ZIP entry is truncated".into(),
            }
            .into());
        }
        file.seek(SeekFrom::Start(data_offset))?;
        let mut compressed = vec![0_u8; compressed_size as usize];
        file.read_exact(&mut compressed)?;
        let payload = decompress_payload(method, &compressed, limit)
            .with_context(|| format!("ZIP entry cannot be salvaged: {name}"))?;
        ensure!(
            payload.len() as u64 == declared_size,
            "recovered ZIP entry has the wrong size"
        );
        ensure!(
            crc32fast::hash(&payload) == expected_crc,
            "recovered ZIP entry failed CRC validation"
        );
        total = total
            .checked_add(payload.len() as u64)
            .context("archive size overflow")?;
        ensure!(total <= limit, "archive expands past the configured limit");
        let target = destination.join(name.trim_end_matches('/'));
        if name.ends_with('/') {
            fs::create_dir_all(target)?;
        } else {
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(target, payload)?;
        }
        recovered += 1;
        ensure!(recovered <= MAX_ENTRIES, "archive has too many entries");
        offset = data_end;
    }
    ensure!(recovered > 0, "no recoverable ZIP entries were found");
    Ok(())
}

fn rebuild(
    candidate: &Path,
    source: &Path,
    root: &Path,
    raw_copy: bool,
    plan: &ActionPlan,
) -> Result<()> {
    let file = File::create(candidate)?;
    let mut output = ZipWriter::new(file);
    let current_files: HashMap<_, _> = files(root)?
        .into_iter()
        .map(|path| {
            (
                path.strip_prefix(root)
                    .unwrap()
                    .to_string_lossy()
                    .replace('\\', "/"),
                path,
            )
        })
        .collect();
    let mut copied = HashSet::new();
    if raw_copy {
        let mut input = ZipArchive::new(File::open(source)?)?;
        for index in 0..input.len() {
            let entry = input.by_index_raw(index)?;
            let name = safe_name(entry.name())?;
            if entry.is_dir() {
                output.add_directory(
                    &name,
                    SimpleFileOptions::default()
                        .unix_permissions(entry.unix_mode().unwrap_or(0o755)),
                )?;
            } else if current_files.contains_key(&name)
                && !plan.skip.contains(name.trim_end_matches('/'))
                && !plan.recover_crc.contains(&name)
                && plan.recover_image.as_deref() != Some(name.as_str())
                && plan.replacement.as_ref().map(|value| value.0.as_str()) != Some(name.as_str())
            {
                copied.insert(name);
                output.raw_copy_file(entry)?;
            }
        }
    }
    let mut current: Vec<_> = current_files
        .into_iter()
        .filter(|(name, _)| !copied.contains(name))
        .collect();
    current.sort_by(|left, right| left.0.cmp(&right.0));
    for (name, path) in current {
        let image = matches!(
            detect(&path)?,
            Format::Jpeg | Format::Png | Format::Gif | Format::Webp | Format::Jxl
        );
        let options = SimpleFileOptions::default()
            .compression_method(if image {
                CompressionMethod::Stored
            } else {
                CompressionMethod::Deflated
            })
            .unix_permissions(0o644);
        output.start_file(name, options)?;
        io::copy(&mut File::open(path)?, &mut output)?;
    }
    output.finish()?.sync_all()?;
    Ok(())
}

async fn verify_candidate(engine: &Engine, candidate: &Path, root: &Path) -> Result<()> {
    let expected: HashSet<_> = files(root)?
        .into_iter()
        .map(|path| {
            path.strip_prefix(root)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/")
        })
        .collect();
    let mut archive = ZipArchive::new(File::open(candidate)?)?;
    ensure!(
        archive.len() <= MAX_ENTRIES,
        "replacement has too many entries"
    );
    let mut actual = HashSet::new();
    for index in 0..archive.len() {
        let mut entry = archive.by_index(index)?;
        if entry.is_dir() {
            continue;
        }
        let name = safe_name(entry.name())?;
        ensure!(actual.insert(name), "replacement has duplicate paths");
        io::copy(&mut entry, &mut io::sink())?;
    }
    ensure!(
        actual == expected,
        "replacement archive contents do not match the workspace"
    );
    let jxl: Vec<_> = files(root)?
        .into_iter()
        .filter(|path| detect(path).is_ok_and(|format| format == Format::Jxl))
        .collect();
    for path in representative(&jxl) {
        engine.media.decode_jxl(path).await?;
    }
    Ok(())
}

async fn verify_extracted(engine: &Engine, images: &[(PathBuf, Format)], deep: bool) -> Result<()> {
    if deep {
        for (path, format) in images.iter().filter(|(_, format)| *format == Format::Jxl) {
            let _ = format;
            engine.media.decode_jxl(path).await?;
        }
    }
    Ok(())
}

async fn verify_recovered_images(engine: &Engine, images: &[(PathBuf, Format)]) -> Result<()> {
    for (path, format) in images {
        if *format == Format::Jxl {
            engine.media.decode_jxl(path).await?;
        } else {
            let decoded = tempfile::Builder::new()
                .suffix(".png")
                .tempfile_in(path.parent().context("recovered image has no parent")?)?;
            engine.media.normalize(path, decoded.path()).await?;
            engine.media.info(decoded.path()).await?;
        }
    }
    Ok(())
}

pub(super) fn fingerprint(path: &Path) -> Result<String> {
    Ok(fingerprints(path)?.0)
}

pub(super) fn fingerprints(path: &Path) -> Result<(String, String)> {
    let mut archive = ZipArchive::new(File::open(path)?)?;
    ensure!(archive.len() <= MAX_ENTRIES, "archive has too many entries");
    let size = path.metadata()?.len();
    let mut current = blake3::Hasher::new();
    current.update(&size.to_le_bytes());
    let mut legacy = Blake2bVar::new(20)?;
    legacy.update(size.to_string().as_bytes());
    for index in 0..archive.len() {
        let entry = archive.by_index_raw(index)?;
        current.update(entry.name().replace('\\', "/").as_bytes());
        current.update(&entry.crc32().to_le_bytes());
        current.update(&entry.size().to_le_bytes());
        current.update(&entry.compressed_size().to_le_bytes());
        let mut name = entry.name_raw().to_vec();
        name.iter_mut().for_each(|byte| {
            if *byte == b'\\' {
                *byte = b'/';
            }
        });
        legacy.update(&name);
        legacy.update(
            format!(
                ":{}:{}:{}:{};",
                entry.crc32(),
                entry.size(),
                entry.compressed_size(),
                compression_code(entry.compression())
            )
            .as_bytes(),
        );
    }
    let mut legacy_digest = [0_u8; 20];
    legacy.finalize_variable(&mut legacy_digest)?;
    Ok((
        current.finalize().to_hex().to_string(),
        hex::encode(legacy_digest),
    ))
}

fn fingerprint_matches(expected: &str, current: &str, legacy: Option<&str>) -> bool {
    expected == current
        || is_legacy_fingerprint(expected) && legacy.is_some_and(|value| value == expected)
}

#[cfg(test)]
pub(super) fn legacy_fingerprint(path: &Path) -> Result<String> {
    Ok(fingerprints(path)?.1)
}

fn is_legacy_fingerprint(value: &str) -> bool {
    value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[allow(deprecated)]
fn compression_code(method: CompressionMethod) -> u16 {
    method.to_u16()
}

pub(super) fn inventory(path: &Path) -> Result<Vec<FormatStat>> {
    let mut archive = ZipArchive::new(File::open(path)?)?;
    ensure!(archive.len() <= MAX_ENTRIES, "archive has too many entries");
    let mut formats: HashMap<String, FormatStat> = HashMap::new();
    for index in 0..archive.len() {
        let entry = archive.by_index_raw(index)?;
        if entry.is_dir() {
            continue;
        }
        let extension = Path::new(entry.name())
            .extension()
            .map(|value| value.to_string_lossy().to_lowercase())
            .unwrap_or_default();
        let label = match extension.as_str() {
            "jpg" | "jpeg" => "JPEG",
            "png" => "PNG",
            "jxl" => "JPEG XL",
            "webp" => "WebP",
            "gif" => "GIF",
            "tif" | "tiff" => "TIFF",
            "bmp" => "BMP",
            "avif" => "AVIF",
            _ => continue,
        };
        let value = formats.entry(label.into()).or_insert_with(|| FormatStat {
            format: label.into(),
            files: 0,
            compressed_bytes: 0,
            uncompressed_bytes: 0,
        });
        value.files += 1;
        value.compressed_bytes += entry.compressed_size();
        value.uncompressed_bytes += entry.size();
    }
    let mut values: Vec<_> = formats.into_values().collect();
    values.sort_by(|left, right| left.format.cmp(&right.format));
    Ok(values)
}

fn guard_fingerprint(path: &Path) -> Result<String> {
    Ok(guard_fingerprints(path)?.0)
}

fn guard_fingerprints(path: &Path) -> Result<(String, Option<String>)> {
    fingerprints(path)
        .map(|(current, legacy)| (current, Some(legacy)))
        .or_else(|_| {
            let mut file = File::open(path)?;
            let mut hash = blake3::Hasher::new();
            let mut buffer = [0_u8; 1024 * 1024];
            loop {
                let count = file.read(&mut buffer)?;
                if count == 0 {
                    break;
                }
                hash.update(&buffer[..count]);
            }
            Ok((hash.finalize().to_hex().to_string(), None))
        })
}

fn detect(path: &Path) -> Result<Format> {
    let mut header = [0_u8; 16];
    let count = File::open(path)?.read(&mut header)?;
    let value = &header[..count];
    Ok(if value.starts_with(&[0xff, 0xd8, 0xff]) {
        Format::Jpeg
    } else if value.starts_with(b"\x89PNG\r\n\x1a\n") {
        Format::Png
    } else if value.starts_with(b"II*\0") || value.starts_with(b"MM\0*") {
        Format::Tiff
    } else if value.starts_with(b"BM") {
        Format::Bmp
    } else if value.len() >= 12 && value.starts_with(b"RIFF") && &value[8..12] == b"WEBP" {
        Format::Webp
    } else if value.starts_with(b"GIF87a") || value.starts_with(b"GIF89a") {
        Format::Gif
    } else if value.starts_with(&[0xff, 0x0a]) || value.starts_with(b"\0\0\0\x0cJXL ") {
        Format::Jxl
    } else {
        Format::Unknown
    })
}

fn safe_name(name: &str) -> Result<String> {
    ensure!(!name.contains('\\'), "non-canonical archive path: {name}");
    let path = Path::new(name);
    ensure!(!path.is_absolute(), "absolute archive path: {name}");
    ensure!(
        path.components()
            .all(|part| matches!(part, Component::Normal(_))),
        "unsafe archive path: {name}"
    );
    ensure!(
        !name
            .split('/')
            .any(|part| part.is_empty() && !name.ends_with('/')),
        "non-canonical archive path: {name}"
    );
    Ok(name.trim_end_matches('/').to_owned() + if name.ends_with('/') { "/" } else { "" })
}

fn files(root: &Path) -> Result<Vec<PathBuf>> {
    WalkDir::new(root)
        .follow_links(false)
        .into_iter()
        .filter_map(|entry| match entry {
            Ok(entry) if entry.file_type().is_file() => Some(Ok(entry.into_path())),
            Ok(_) => None,
            Err(error) => Some(Err(error.into())),
        })
        .collect()
}

fn workspace(root: &Path) -> Result<TempDir> {
    fs::create_dir_all(root)?;
    Ok(tempfile::Builder::new()
        .prefix("archive-")
        .tempdir_in(root)?)
}

fn temporary_candidate(source: &Path) -> Result<TempPath> {
    let temporary = tempfile::Builder::new()
        .suffix(".jxl")
        .tempfile_in(source.parent().unwrap())?
        .into_temp_path();
    fs::remove_file(&temporary)?;
    Ok(temporary)
}

fn candidate_path(source: &Path, job_id: &str) -> PathBuf {
    source.with_file_name(format!(
        ".{}.{}.cbzxl-partial",
        source.file_name().unwrap().to_string_lossy(),
        &job_id[..job_id.len().min(12)]
    ))
}

fn identity(path: &Path) -> Result<Identity> {
    let metadata = path.metadata()?;
    #[cfg(unix)]
    use std::os::unix::fs::MetadataExt;
    Ok(Identity {
        size: metadata.len(),
        modified: metadata.modified().ok(),
        #[cfg(unix)]
        device: metadata.dev(),
        #[cfg(unix)]
        inode: metadata.ino(),
    })
}

fn require_unchanged(path: &Path, expected: &Identity, expected_fingerprint: &str) -> Result<()> {
    ensure!(
        &identity(path)? == expected,
        "source archive changed during processing"
    );
    ensure!(
        guard_fingerprint(path)? == expected_fingerprint,
        "source archive contents changed during processing"
    );
    Ok(())
}

fn stable(identity: &Identity, duration: Duration) -> bool {
    duration.is_zero()
        || identity
            .modified
            .is_some_and(|modified| modified.elapsed().is_ok_and(|elapsed| elapsed >= duration))
}

fn fingerprint_due(path: &str, days: u64) -> bool {
    let day = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |value| value.as_secs() / 86_400);
    u64::from(crc32fast::hash(path.as_bytes())) % days.max(1) == day % days.max(1)
}

fn modified_ns(value: SystemTime) -> Option<i64> {
    value
        .duration_since(SystemTime::UNIX_EPOCH)
        .ok()
        .map(|value| value.as_nanos().min(i64::MAX as u128) as i64)
}

fn mtime_matches(expected: Option<i64>, current: Option<i64>, fingerprint: Option<&str>) -> bool {
    expected == current
        || (fingerprint.is_none() || fingerprint.is_some_and(is_legacy_fingerprint))
            && expected
                .zip(current)
                .is_some_and(|(left, right)| left.abs_diff(right) < 1_000_000)
}

fn is_symlink(mode: Option<u32>) -> bool {
    mode.is_some_and(|mode| mode & 0o170000 == 0o120000)
}

fn parallelism(cpu: usize, pages: usize) -> (usize, usize) {
    let workers = pages.max(1).min(cpu.max(1)).min(8);
    (workers, (cpu.max(1) / workers).max(1))
}

fn distances(
    policy: &crate::domain::Policy,
    size: u64,
    info: &ImageInfo,
) -> Vec<(f32, &'static str)> {
    match policy.strategy {
        Strategy::Lossless => vec![(0.0, "Lossless")],
        Strategy::Lossy => vec![(policy.distance.unwrap_or(0.5), "Lossy")],
        Strategy::Smart => {
            let mut values = vec![(0.0, "Smart Lossless")];
            if size as f32 > policy.smart_mb * 1024.0 * 1024.0
                || info.megapixels() > policy.smart_mp
            {
                values.push((policy.distance.unwrap_or(0.5), "Smart Lossy"));
            }
            values
        }
        _ => Vec::new(),
    }
}

fn representative<T>(values: &[T]) -> Vec<&T> {
    if values.is_empty() {
        Vec::new()
    } else {
        let indexes = [0, values.len() / 2, values.len() - 1];
        let mut seen = HashSet::new();
        indexes
            .into_iter()
            .filter(|index| seen.insert(*index))
            .map(|index| &values[index])
            .collect()
    }
}

fn strategy_name(strategy: Strategy) -> &'static str {
    match strategy {
        Strategy::Lossless => "Lossless",
        Strategy::Smart => "Smart",
        Strategy::Lossy => "Lossy",
        Strategy::Recheck => "Recheck",
        Strategy::Audit => "Integrity audit",
    }
}

fn extension_overhead(pages: &[PageResult]) -> u64 {
    pages
        .iter()
        .filter(|page| page.outcome == "extension_corrected")
        .map(|page| {
            let extension = match page.source_format.as_deref() {
                Some("image/jpeg") => "jpg",
                Some("image/png") => "png",
                Some("image/tiff") => "tiff",
                Some("image/bmp") => "bmp",
                Some("image/webp") => "webp",
                Some("image/gif") => "gif",
                _ => "",
            };
            let path = Path::new(&page.entry_path);
            let stem = path
                .file_stem()
                .map(|value| value.to_string_lossy().len())
                .unwrap_or(0);
            let parent = path
                .parent()
                .filter(|value| !value.as_os_str().is_empty())
                .map(|value| value.to_string_lossy().len() + 1)
                .unwrap_or(0);
            let corrected = parent + stem + 1 + extension.len();
            corrected.saturating_sub(page.entry_path.len()) as u64 * 2
        })
        .sum()
}

fn sync_parent(path: &Path) -> Result<()> {
    #[cfg(unix)]
    File::open(path.parent().context("archive has no parent")?)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::Config,
        db::Database,
        domain::{JobSource, Policy},
        engine::{MediaBackend, MediaCandidate, MediaRequest},
        progress::ProgressHub,
    };
    use async_trait::async_trait;
    use std::{io::Write, sync::Arc};

    #[derive(Default)]
    struct MockMedia;

    #[derive(Default)]
    struct RetryMedia(std::sync::atomic::AtomicUsize);

    #[derive(Default)]
    struct EffortMedia(std::sync::Mutex<Vec<u8>>);

    #[async_trait]
    impl MediaBackend for MockMedia {
        fn doctor(&self) -> Result<()> {
            Ok(())
        }

        async fn info(&self, _source: &Path) -> Result<ImageInfo> {
            Ok(ImageInfo {
                width: 100,
                height: 100,
                colorspace: "sRGB".into(),
            })
        }

        async fn normalize(&self, source: &Path, destination: &Path) -> Result<()> {
            fs::copy(source, destination)?;
            Ok(())
        }

        async fn signature(&self, _source: &Path) -> Result<String> {
            Ok("mock pixels".into())
        }

        async fn encode(&self, request: MediaRequest) -> Result<MediaCandidate> {
            let mut payload = vec![0xff, 0x0a];
            payload.extend([0_u8; 62]);
            fs::write(&request.destination, payload)?;
            Ok(MediaCandidate {
                path: request.destination,
                size: 64,
            })
        }

        async fn verify(
            &self,
            _source: &Path,
            _candidate: &Path,
            _lossless: bool,
            _reconstruct_jpeg: bool,
            _source_signature: Option<&str>,
        ) -> Result<()> {
            Ok(())
        }

        async fn decode_jxl(&self, _source: &Path) -> Result<()> {
            Ok(())
        }
    }

    #[async_trait]
    impl MediaBackend for RetryMedia {
        fn doctor(&self) -> Result<()> {
            Ok(())
        }
        async fn info(&self, _: &Path) -> Result<ImageInfo> {
            Ok(ImageInfo {
                width: 100,
                height: 100,
                colorspace: "sRGB".into(),
            })
        }
        async fn normalize(&self, source: &Path, destination: &Path) -> Result<()> {
            fs::copy(source, destination)?;
            Ok(())
        }
        async fn signature(&self, _: &Path) -> Result<String> {
            Ok("mock pixels".into())
        }
        async fn encode(&self, request: MediaRequest) -> Result<MediaCandidate> {
            if self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed) == 0 {
                anyhow::bail!("temporary encoder failure");
            }
            fs::write(&request.destination, [0xff, 0x0a, 0, 0])?;
            Ok(MediaCandidate {
                path: request.destination,
                size: 4,
            })
        }
        async fn verify(
            &self,
            _: &Path,
            _: &Path,
            _: bool,
            _: bool,
            _: Option<&str>,
        ) -> Result<()> {
            Ok(())
        }
        async fn decode_jxl(&self, _: &Path) -> Result<()> {
            Ok(())
        }
    }

    #[async_trait]
    impl MediaBackend for EffortMedia {
        fn doctor(&self) -> Result<()> {
            Ok(())
        }
        async fn info(&self, _: &Path) -> Result<ImageInfo> {
            Ok(ImageInfo {
                width: 100,
                height: 100,
                colorspace: "sRGB".into(),
            })
        }
        async fn normalize(&self, source: &Path, destination: &Path) -> Result<()> {
            fs::copy(source, destination)?;
            Ok(())
        }
        async fn signature(&self, _: &Path) -> Result<String> {
            Ok("pixels".into())
        }
        async fn encode(&self, request: MediaRequest) -> Result<MediaCandidate> {
            self.0.lock().unwrap().push(request.effort);
            let size = request.source.metadata()?.len();
            fs::write(&request.destination, vec![0_u8; size as usize])?;
            Ok(MediaCandidate {
                path: request.destination,
                size,
            })
        }
        async fn verify(
            &self,
            _: &Path,
            _: &Path,
            _: bool,
            _: bool,
            _: Option<&str>,
        ) -> Result<()> {
            Ok(())
        }
        async fn decode_jxl(&self, _: &Path) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn unsafe_archive_names_are_rejected() {
        assert!(safe_name("../outside.jpg").is_err());
        assert!(safe_name("/absolute.jpg").is_err());
        assert!(safe_name("folder\\page.jpg").is_err());
        assert!(safe_name("./page.jpg").is_err());
        assert_eq!(safe_name("folder/page.jpg").unwrap(), "folder/page.jpg");
    }

    #[test]
    fn imported_v4_mtime_rounding_still_fast_skips() {
        let legacy = "0123456789abcdef0123456789abcdef01234567";
        assert!(mtime_matches(
            Some(1_750_000_000_123_456_000),
            Some(1_750_000_000_123_456_512),
            Some(legacy)
        ));
        assert!(!mtime_matches(
            Some(1_750_000_000_123_456_000),
            Some(1_750_000_000_124_456_000),
            Some(legacy)
        ));
        assert!(mtime_matches(
            Some(1_750_000_000_123_456_000),
            Some(1_750_000_000_123_999_999),
            None
        ));
        assert!(!mtime_matches(
            Some(1_750_000_000_123_456_000),
            Some(1_750_000_000_123_456_512),
            Some(&"a".repeat(64))
        ));
    }

    #[test]
    fn webp_is_inventory_only_and_never_a_conversion_candidate() {
        assert!(!Format::Webp.convertible());
        assert!(Format::Jpeg.convertible());
        assert!(Format::Png.convertible());
    }

    #[tokio::test]
    async fn selected_effort_is_a_hard_ceiling() -> Result<()> {
        let root = tempfile::tempdir()?;
        let source = root.path().join("001.jpg");
        fs::write(&source, vec![7_u8; 1_024])?;
        let media = std::sync::Arc::new(EffortMedia::default());
        let page = PageWork {
            path: source,
            entry: "001.jpg".into(),
            format: Format::Jpeg,
            force_normalize: false,
        };
        let policy = Policy {
            effort: 8,
            ..Policy::default()
        };
        let result = convert_page(media.clone(), policy, page, 1).await?;
        assert!(!result.applied);
        assert_eq!(*media.0.lock().unwrap(), vec![8]);
        Ok(())
    }

    #[tokio::test]
    async fn webp_archive_never_reaches_the_encoder() -> Result<()> {
        let root = tempfile::tempdir()?;
        let library_root = root.path().join("library");
        fs::create_dir_all(&library_root)?;
        let archive_path = library_root.join("WebP.cbz");
        {
            let mut archive = ZipWriter::new(File::create(&archive_path)?);
            archive.start_file(
                "001.webp",
                SimpleFileOptions::default().compression_method(CompressionMethod::Stored),
            )?;
            archive.write_all(b"RIFF\x10\0\0\0WEBPVP8 ")?;
            archive.finish()?;
        }
        let database = Database::open(root.path().join("cbzxl.sqlite3"))?;
        database.migrate()?;
        database.connect()?.execute(
            "UPDATE libraries SET root_path=? WHERE id='library-default'",
            [library_root.to_string_lossy().as_ref()],
        )?;
        database.enqueue(
            "library-default",
            Some("profile-balanced"),
            &Policy::default(),
            JobSource::Manual,
            None,
        )?;
        let job = database.claim()?.unwrap();
        let library = database.libraries()?.into_iter().next().unwrap();
        let media = Arc::new(RetryMedia::default());
        let engine = Engine {
            config: test_config(root.path(), database.path().to_owned(), library_root),
            database,
            media: media.clone(),
            progress: ProgressHub::default(),
        };
        let result = process(&engine, &job, &library, &archive_path, "WebP.cbz").await?;
        assert_eq!(result.result.outcome, Outcome::Retained);
        assert_eq!(media.0.load(std::sync::atomic::Ordering::Relaxed), 0);
        Ok(())
    }

    #[tokio::test]
    async fn imported_archive_without_fingerprint_uses_millisecond_stat_tolerance() -> Result<()> {
        let root = tempfile::tempdir()?;
        let library_root = root.path().join("library");
        fs::create_dir_all(&library_root)?;
        let archive_path = library_root.join("Imported.cbz");
        {
            let mut archive = ZipWriter::new(File::create(&archive_path)?);
            archive.start_file(
                "001.jpg",
                SimpleFileOptions::default().compression_method(CompressionMethod::Stored),
            )?;
            archive.write_all(&vec![5_u8; 4_096])?;
            archive.finish()?;
        }
        let current_mtime = identity(&archive_path)?
            .modified
            .and_then(modified_ns)
            .unwrap();
        let size = archive_path.metadata()?.len();
        let database = Database::open(root.path().join("cbzxl.sqlite3"))?;
        database.migrate()?;
        database.connect()?.execute(
            "UPDATE libraries SET root_path=? WHERE id='library-default'",
            [library_root.to_string_lossy().as_ref()],
        )?;
        let timestamp = crate::db::now();
        database.connect()?.execute(
            "INSERT INTO archives(library_id,path,status,current_size,mtime_ns,created_at,updated_at) VALUES ('library-default','Imported.cbz','processed',?,?,?,?)",
            rusqlite::params![size as i64,current_mtime-500_000,timestamp,timestamp],
        )?;
        database.enqueue(
            "library-default",
            Some("profile-balanced"),
            &Policy::default(),
            JobSource::Manual,
            None,
        )?;
        let job = database.claim()?.unwrap();
        let library = database.libraries()?.into_iter().next().unwrap();
        let media = Arc::new(EffortMedia::default());
        let mut config = test_config(root.path(), database.path().to_owned(), library_root);
        config.fingerprint_audit_days = u64::MAX;
        let engine = Engine {
            config,
            database,
            media: media.clone(),
            progress: ProgressHub::default(),
        };
        let result = process(&engine, &job, &library, &archive_path, "Imported.cbz").await?;
        assert_eq!(result.result.outcome, Outcome::AlreadyCurrent);
        assert!(result.refresh_identity);
        assert!(media.0.lock().unwrap().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn legacy_fingerprint_upgrade_preserves_baseline_and_attention() -> Result<()> {
        let root = tempfile::tempdir()?;
        let library_root = root.path().join("library");
        fs::create_dir_all(&library_root)?;
        let archive_path = library_root.join("Legacy.cbz");
        {
            let mut archive = ZipWriter::new(File::create(&archive_path)?);
            archive.start_file(
                "001.jpg",
                SimpleFileOptions::default().compression_method(CompressionMethod::Stored),
            )?;
            archive.write_all(&vec![3_u8; 4_096])?;
            archive.finish()?;
        }
        let size = archive_path.metadata()?.len();
        let mtime = identity(&archive_path)?
            .modified
            .and_then(modified_ns)
            .unwrap();
        let legacy = legacy_fingerprint(&archive_path)?;
        let database = Database::open(root.path().join("cbzxl.sqlite3"))?;
        database.migrate()?;
        database.connect()?.execute(
            "UPDATE libraries SET root_path=? WHERE id='library-default'",
            [library_root.to_string_lossy().as_ref()],
        )?;
        let timestamp = crate::db::now();
        let connection = database.connect()?;
        connection.execute(
            "INSERT INTO archives(library_id,path,status,baseline_size,current_size,bytes_saved,mtime_ns,fingerprint,created_at,updated_at) VALUES ('library-default','Legacy.cbz','processed',?,?,1000,?,?,?,?)",
            rusqlite::params![size as i64+1000,size as i64,mtime-2_000_000,legacy,timestamp,timestamp],
        )?;
        let archive_id = connection.last_insert_rowid();
        connection.execute(
            "INSERT INTO issues(archive_id,kind,stage,message,status,created_at,updated_at) VALUES (?,'archive_grew','verifying','Keep this issue','open',?,?)",
            rusqlite::params![archive_id,timestamp,timestamp],
        )?;
        let job_id = database.enqueue(
            "library-default",
            Some("profile-balanced"),
            &Policy::default(),
            JobSource::Manual,
            None,
        )?;
        let job = database.claim()?.unwrap();
        let library = database.libraries()?.into_iter().next().unwrap();
        let media = Arc::new(EffortMedia::default());
        let engine = Engine {
            config: test_config(root.path(), database.path().to_owned(), library_root),
            database: database.clone(),
            media: media.clone(),
            progress: ProgressHub::default(),
        };
        let record = process(&engine, &job, &library, &archive_path, "Legacy.cbz").await?;
        assert_eq!(record.result.outcome, Outcome::AlreadyCurrent);
        assert!(record.refresh_identity);
        database.record_batch(&job_id, "library-default", &[record])?;
        let connection = database.connect()?;
        let (baseline, current, fingerprint): (i64, i64, String) = connection.query_row(
            "SELECT baseline_size,current_size,fingerprint FROM archives WHERE id=?",
            [archive_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        assert_eq!((baseline, current), (size as i64 + 1000, size as i64));
        assert_eq!(fingerprint.len(), 64);
        assert_eq!(
            connection.query_row(
                "SELECT status FROM issues WHERE archive_id=?",
                [archive_id],
                |row| row.get::<_, String>(0)
            )?,
            "open"
        );
        assert_eq!(
            connection.query_row(
                "SELECT COUNT(*) FROM job_archive_outcomes WHERE job_id=?",
                [job_id],
                |row| row.get::<_, i64>(0)
            )?,
            0
        );
        assert!(media.0.lock().unwrap().is_empty());
        Ok(())
    }

    #[test]
    fn rebuild_does_not_raw_copy_over_a_replacement() -> Result<()> {
        let root = tempfile::tempdir()?;
        let source = root.path().join("source.cbz");
        {
            let mut archive = ZipWriter::new(File::create(&source)?);
            archive.start_file(
                "001.png",
                SimpleFileOptions::default().compression_method(CompressionMethod::Stored),
            )?;
            archive.write_all(b"original")?;
            archive.finish()?;
        }
        let workspace = root.path().join("workspace");
        fs::create_dir(&workspace)?;
        fs::write(workspace.join("001.png"), b"replacement")?;
        let staged = root.path().join("replacement.png");
        fs::write(&staged, b"replacement")?;
        let plan = ActionPlan {
            replacement: Some(("001.png".into(), staged)),
            ..ActionPlan::default()
        };
        let candidate = root.path().join("candidate.cbz");
        rebuild(&candidate, &source, &workspace, true, &plan)?;
        let mut archive = ZipArchive::new(File::open(candidate)?)?;
        let mut payload = Vec::new();
        archive.by_name("001.png")?.read_to_end(&mut payload)?;
        assert_eq!(payload, b"replacement");
        Ok(())
    }

    #[test]
    fn computes_the_v4_archive_fingerprint() -> Result<()> {
        let root = tempfile::tempdir()?;
        let source = root.path().join("legacy.cbz");
        {
            let mut archive = ZipWriter::new(File::create(&source)?);
            archive.start_file(
                "folder/page.jpg",
                SimpleFileOptions::default().compression_method(CompressionMethod::Stored),
            )?;
            archive.write_all(b"legacy pixels")?;
            archive.finish()?;
        }
        assert_eq!(
            legacy_fingerprint(&source)?,
            "f1337690620636beda4f7d72693acecda4eda741"
        );
        Ok(())
    }

    #[test]
    fn recovery_supports_bzip2_lzma_and_zip64() -> Result<()> {
        let payload = b"recover every supported ZIP payload";
        let mut bzip = bzip2::write::BzEncoder::new(Vec::new(), bzip2::Compression::best());
        bzip.write_all(payload)?;
        assert_eq!(decompress_payload(12, &bzip.finish()?, 1024)?, payload);

        let options = liblzma::stream::LzmaOptions::new_preset(6)?;
        let stream = liblzma::stream::Stream::new_lzma_encoder(&options)?;
        let mut lzma = liblzma::write::XzEncoder::new_stream(Vec::new(), stream);
        lzma.write_all(payload)?;
        assert_eq!(decompress_payload(14, &lzma.finish()?, 1024)?, payload);

        let mut extra = Vec::new();
        extra.extend_from_slice(&1_u16.to_le_bytes());
        extra.extend_from_slice(&16_u16.to_le_bytes());
        extra.extend_from_slice(&1234_u64.to_le_bytes());
        extra.extend_from_slice(&567_u64.to_le_bytes());
        assert_eq!(
            zip64_sizes(&extra, u32::MAX as u64, u32::MAX as u64)?,
            (567, 1234)
        );
        assert_eq!(
            oem_cp::decode_string_complete_table(
                b"Cura\x87ao",
                &oem_cp::code_table::DECODING_TABLE_CP437
            ),
            "Curaçao"
        );
        Ok(())
    }

    #[tokio::test]
    async fn transient_page_failures_receive_a_compatibility_retry() -> Result<()> {
        let root = tempfile::tempdir()?;
        let page = root.path().join("page.png");
        fs::write(
            &page,
            [b"\x89PNG\r\n\x1a\n".as_slice(), &[7_u8; 256]].concat(),
        )?;
        let result = convert_page(
            Arc::new(RetryMedia::default()),
            Policy::default(),
            PageWork {
                path: page,
                entry: "page.png".into(),
                format: Format::Png,
                force_normalize: false,
            },
            2,
        )
        .await?;
        assert_eq!(result.result.outcome, "converted");
        assert!(
            result
                .result
                .attempts
                .iter()
                .any(|attempt| attempt.get("error").is_some())
        );
        Ok(())
    }

    #[test]
    fn parallelism_stays_within_the_budget() {
        for cpu in 1..32 {
            for pages in 1..100 {
                let (workers, threads) = parallelism(cpu, pages);
                assert!(workers * threads <= cpu);
                assert!(workers <= pages);
            }
        }
    }

    #[test]
    fn local_headers_salvage_an_archive_without_a_central_directory() -> Result<()> {
        let root = tempfile::tempdir()?;
        let source = root.path().join("broken.cbz");
        {
            let mut archive = ZipWriter::new(File::create(&source)?);
            for (name, value) in [
                ("001.jpg", b"first".as_slice()),
                ("002.jpg", b"second".as_slice()),
            ] {
                archive.start_file(
                    name,
                    SimpleFileOptions::default().compression_method(CompressionMethod::Stored),
                )?;
                archive.write_all(value)?;
            }
            archive.finish()?;
        }
        let bytes = fs::read(&source)?;
        let central = bytes
            .windows(4)
            .position(|value| value == b"PK\x01\x02")
            .unwrap();
        fs::write(&source, &bytes[..central])?;
        let recovered = root.path().join("recovered");
        fs::create_dir(&recovered)?;
        extract_salvage(&source, &recovered, 1024, false)?;
        assert_eq!(fs::read(recovered.join("001.jpg"))?, b"first");
        assert_eq!(fs::read(recovered.join("002.jpg"))?, b"second");
        Ok(())
    }

    #[test]
    fn crc_recovery_reads_payload_without_trusting_stored_crc() -> Result<()> {
        let root = tempfile::tempdir()?;
        let source = root.path().join("crc.cbz");
        {
            let mut archive = ZipWriter::new(File::create(&source)?);
            archive.start_file(
                "001.jpg",
                SimpleFileOptions::default().compression_method(CompressionMethod::Stored),
            )?;
            archive.write_all(b"recoverable pixels")?;
            archive.finish()?;
        }
        let mut bytes = fs::read(&source)?;
        let local = bytes
            .windows(4)
            .position(|value| value == b"PK\x03\x04")
            .unwrap();
        let central = bytes
            .windows(4)
            .position(|value| value == b"PK\x01\x02")
            .unwrap();
        bytes[local + 14] ^= 0xff;
        bytes[central + 16] ^= 0xff;
        fs::write(&source, bytes)?;
        let destination = root.path().join("files");
        fs::create_dir(&destination)?;
        let mut plan = ActionPlan::default();
        plan.recover_crc.insert("001.jpg".into());
        extract(&source, &destination, 1024, &plan)?;
        assert_eq!(
            fs::read(destination.join("001.jpg"))?,
            b"recoverable pixels"
        );
        Ok(())
    }

    #[tokio::test]
    async fn verified_smaller_archive_is_atomically_replaced() -> Result<()> {
        let root = tempfile::tempdir()?;
        let library_root = root.path().join("library");
        let scratch = root.path().join("scratch");
        fs::create_dir_all(&library_root)?;
        let archive_path = library_root.join("Book.cbz");
        {
            let mut archive = ZipWriter::new(File::create(&archive_path)?);
            archive.start_file(
                "pages/001.png",
                SimpleFileOptions::default().compression_method(CompressionMethod::Stored),
            )?;
            archive.write_all(b"\x89PNG\r\n\x1a\n")?;
            archive.write_all(&vec![7_u8; 8192])?;
            archive.start_file(
                "ComicInfo.xml",
                SimpleFileOptions::default().compression_method(CompressionMethod::Deflated),
            )?;
            archive.write_all(&vec![b'x'; 4096])?;
            archive.finish()?;
        }
        let unchanged_before = {
            let mut archive = ZipArchive::new(File::open(&archive_path)?)?;
            let entry = archive.by_name("ComicInfo.xml")?;
            (entry.crc32(), entry.compressed_size(), entry.compression())
        };

        let database = Database::open(root.path().join("cbzxl.sqlite3"))?;
        database.migrate()?;
        database.connect()?.execute(
            "UPDATE libraries SET root_path=? WHERE id='library-default'",
            [library_root.to_string_lossy().as_ref()],
        )?;
        let policy = Policy::default();
        database.enqueue(
            "library-default",
            Some("profile-balanced"),
            &policy,
            JobSource::Manual,
            None,
        )?;
        let job = database.claim()?.expect("queued job");
        let library = database.libraries()?.into_iter().next().unwrap();
        let engine = Engine {
            config: Config {
                database: database.path().to_owned(),
                library: library_root,
                runtime: root.path().join("runtime"),
                scratch,
                frontend: root.path().join("frontend"),
                listen: "127.0.0.1:0".into(),
                api_token: None,
                timezone: "UTC".into(),
                cpu_limit: 2,
                archive_limit: 32 * 1024 * 1024,
                stable_for: Duration::ZERO,
                poll_every: Duration::from_millis(10),
                encoder_timeout: Duration::ZERO,
                auxiliary_timeout: Duration::from_secs(30),
                fingerprint_audit_days: 30,
                ntfy_url: None,
                ntfy_token: None,
                ntfy_timeout: Duration::from_secs(10),
                page_outcome_retention_days: 180,
                durable_checkpoint: Duration::from_secs(300),
                durable_checkpoint_archives: 10_000,
                database_maintenance: Duration::from_secs(604_800),
                statistics_cache: Duration::from_secs(30),
                worker_stall: Duration::from_secs(900),
                auto_retry_attempts: 1,
            },
            database: database.clone(),
            media: Arc::new(MockMedia),
            progress: ProgressHub::default(),
        };

        let result = process(&engine, &job, &library, &archive_path, "Book.cbz").await?;
        assert_eq!(result.result.outcome, Outcome::Optimized);
        assert!(result.result.final_size < result.result.source_size);
        let mut archive = ZipArchive::new(File::open(&archive_path)?)?;
        assert!(archive.by_name("pages/001.jxl").is_ok());
        assert!(archive.by_name("pages/001.png").is_err());
        let unchanged = archive.by_name("ComicInfo.xml")?;
        assert_eq!(
            (
                unchanged.crc32(),
                unchanged.compressed_size(),
                unchanged.compression()
            ),
            unchanged_before
        );
        Ok(())
    }

    #[tokio::test]
    async fn explicit_delete_rebuilds_only_the_selected_entry() -> Result<()> {
        let root = tempfile::tempdir()?;
        let library_root = root.path().join("library");
        fs::create_dir_all(&library_root)?;
        let archive_path = library_root.join("Repair.cbz");
        {
            let mut archive = ZipWriter::new(File::create(&archive_path)?);
            for name in ["pages/001.png", "pages/002.png"] {
                archive.start_file(
                    name,
                    SimpleFileOptions::default().compression_method(CompressionMethod::Stored),
                )?;
                archive.write_all(b"\x89PNG\r\n\x1a\n")?;
                archive.write_all(&vec![7_u8; 4096])?;
            }
            archive.finish()?;
        }
        let database = Database::open(root.path().join("cbzxl.sqlite3"))?;
        database.migrate()?;
        database.connect()?.execute(
            "UPDATE libraries SET root_path=? WHERE id='library-default'",
            [library_root.to_string_lossy().as_ref()],
        )?;
        let policy = Policy::default();
        database.enqueue_repair(
            "library-default",
            Some("profile-balanced"),
            &policy,
            "Repair.cbz",
            &RepairAction::Delete {
                entry: "pages/001.png".into(),
            },
        )?;
        let job = database.claim()?.unwrap();
        let library = database.libraries()?.into_iter().next().unwrap();
        let engine = Engine {
            config: test_config(root.path(), database.path().to_owned(), library_root),
            database,
            media: Arc::new(MockMedia),
            progress: ProgressHub::default(),
        };
        let result = process(&engine, &job, &library, &archive_path, "Repair.cbz").await?;
        assert_eq!(result.result.outcome, Outcome::Repaired);
        let mut archive = ZipArchive::new(File::open(&archive_path)?)?;
        assert!(archive.by_name("pages/001.png").is_err());
        assert!(archive.by_name("pages/002.jxl").is_ok());
        Ok(())
    }

    fn test_config(root: &Path, database: PathBuf, library: PathBuf) -> Config {
        Config {
            database,
            library,
            runtime: root.join("runtime"),
            scratch: root.join("scratch"),
            frontend: root.join("frontend"),
            listen: "127.0.0.1:0".into(),
            api_token: None,
            timezone: "UTC".into(),
            cpu_limit: 2,
            archive_limit: 32 * 1024 * 1024,
            stable_for: Duration::ZERO,
            poll_every: Duration::from_millis(10),
            encoder_timeout: Duration::ZERO,
            auxiliary_timeout: Duration::from_secs(30),
            fingerprint_audit_days: 30,
            ntfy_url: None,
            ntfy_token: None,
            ntfy_timeout: Duration::from_secs(10),
            page_outcome_retention_days: 180,
            durable_checkpoint: Duration::from_secs(300),
            durable_checkpoint_archives: 10_000,
            database_maintenance: Duration::from_secs(604_800),
            statistics_cache: Duration::from_secs(30),
            worker_stall: Duration::from_secs(900),
            auto_retry_attempts: 1,
        }
    }
}
