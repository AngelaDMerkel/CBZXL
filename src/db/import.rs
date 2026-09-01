use super::{Database, now};
use crate::domain::{Policy, RepairAction, Scope, Strategy};
use anyhow::{Context, Result, bail, ensure};
use rusqlite::{Connection, OpenFlags, OptionalExtension, Transaction, backup::Backup, params};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashSet},
    fs::File,
    io::Read,
    path::Path,
    time::Duration,
};
use tempfile::{NamedTempFile, TempPath};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceKind {
    CurrentV3,
    LegacyV2,
}

#[derive(Debug, Serialize)]
pub struct ImportReport {
    pub source: String,
    source_kind: SourceKind,
    pub digest: String,
    pub integrity: String,
    pub dry_run: bool,
    pub already_imported: bool,
    pub counts: BTreeMap<String, u64>,
    pub warnings: Vec<String>,
}

pub fn run(database: &Database, source_path: &Path, dry_run: bool) -> Result<ImportReport> {
    ensure!(
        source_path.is_file(),
        "database not found: {}",
        source_path.display()
    );
    ensure!(
        source_path.canonicalize()? != database.path().canonicalize().unwrap_or_default(),
        "source and target databases must be different files"
    );

    let snapshot_path = snapshot(source_path)?;
    let source = readonly(&snapshot_path)?;
    let integrity: String = source.query_row("PRAGMA quick_check", [], |row| row.get(0))?;
    ensure!(
        integrity == "ok",
        "source database failed quick_check: {integrity}"
    );
    let kind = detect(&source)?;
    let counts = source_counts(&source, kind)?;
    let digest = digest(&snapshot_path)?;
    let mut report = ImportReport {
        source: source_path.display().to_string(),
        source_kind: kind,
        digest: digest.clone(),
        integrity,
        dry_run,
        already_imported: false,
        counts,
        warnings: Vec::new(),
    };
    if dry_run {
        return Ok(report);
    }

    let mut target = database.connect()?;
    if target
        .query_row(
            "SELECT 1 FROM imports WHERE source_digest=? AND status='completed'",
            [&digest],
            |_| Ok(()),
        )
        .optional()?
        .is_some()
    {
        report.already_imported = true;
        return Ok(report);
    }
    let occupied: i64 = target.query_row(
        "SELECT (SELECT COUNT(*) FROM archives) + (SELECT COUNT(*) FROM jobs)",
        [],
        |row| row.get(0),
    )?;
    ensure!(occupied == 0, "target already contains jobs or archives");

    let transaction = target.transaction()?;
    match kind {
        SourceKind::CurrentV3 => import_current(&source, &transaction, &mut report)?,
        SourceKind::LegacyV2 => import_legacy(&source, &transaction, &mut report)?,
    }
    transaction.execute(
        "INSERT INTO imports(id,source_path,source_kind,source_digest,status,counts_json,warnings_json,started_at,finished_at) VALUES (?,?,?,?, 'completed',?,?,?,?)",
        params![
            Uuid::new_v4().to_string(),
            report.source,
            serde_json::to_value(kind)?.as_str().unwrap_or("unknown"),
            digest,
            serde_json::to_string(&report.counts)?,
            serde_json::to_string(&report.warnings)?,
            now(),
            now(),
        ],
    )?;
    transaction.commit()?;
    let check = database.doctor()?;
    ensure!(
        check.integrity == "ok",
        "target failed quick_check after import"
    );
    ensure!(
        check.foreign_key_errors == 0,
        "target has foreign-key errors after import"
    );
    Ok(report)
}

fn snapshot(path: &Path) -> Result<TempPath> {
    let source = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    let temporary = NamedTempFile::new()?.into_temp_path();
    let mut destination = Connection::open(&temporary)?;
    Backup::new(&source, &mut destination)?.run_to_completion(
        256,
        Duration::from_millis(10),
        None,
    )?;
    drop(destination);
    Ok(temporary)
}

fn readonly(path: &Path) -> Result<Connection> {
    Ok(Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?)
}

fn digest(path: &Path) -> Result<String> {
    let mut file = File::open(path)?;
    let mut hash = Sha256::new();
    let mut buffer = [0_u8; 1024 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
    }
    Ok(hex::encode(hash.finalize()))
}

fn detect(source: &Connection) -> Result<SourceKind> {
    if table(source, "libraries")? && table(source, "archives")? && table(source, "jobs")? {
        let version: i64 = source
            .query_row(
                "SELECT COALESCE(MAX(version),0) FROM schema_migrations",
                [],
                |row| row.get(0),
            )
            .unwrap_or_default();
        ensure!(
            version >= 15,
            "current database must be upgraded to schema v15 before import"
        );
        return Ok(SourceKind::CurrentV3);
    }
    if table(source, "converted_archives")? {
        return Ok(SourceKind::LegacyV2);
    }
    bail!("database is not a recognized CBZXL schema")
}

fn table(connection: &Connection, name: &str) -> Result<bool> {
    Ok(connection
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            [name],
            |_| Ok(()),
        )
        .optional()?
        .is_some())
}

fn count(connection: &Connection, table: &str) -> Result<u64> {
    let count: i64 = connection
        .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get(0)
        })
        .with_context(|| format!("could not count {table}"))?;
    Ok(count.max(0) as u64)
}

fn source_counts(source: &Connection, kind: SourceKind) -> Result<BTreeMap<String, u64>> {
    let tables: &[&str] = match kind {
        SourceKind::CurrentV3 => &[
            "libraries",
            "conversion_profiles",
            "schedules",
            "jobs",
            "archives",
            "archive_results",
            "page_decisions",
            "archive_issues",
            "review_actions",
            "archive_format_stats",
            "job_events",
            "library_stat_snapshots",
            "archive_commit_intents",
        ],
        SourceKind::LegacyV2 => &["converted_archives", "runs", "archive_results"],
    };
    let mut counts = BTreeMap::new();
    for table_name in tables {
        if table(source, table_name)? {
            counts.insert((*table_name).into(), count(source, table_name)?);
        }
    }
    Ok(counts)
}

fn import_current(
    source: &Connection,
    target: &Transaction<'_>,
    report: &mut ImportReport,
) -> Result<()> {
    copy_libraries(source, target)?;
    copy_policies(source, target)?;
    copy_schedules(source, target)?;
    copy_jobs(source, target)?;
    copy_archives(source, target)?;
    copy_outcomes(source, target)?;
    copy_pages(source, target)?;
    copy_issues(source, target)?;
    copy_reviews(source, target)?;
    copy_formats(source, target)?;
    copy_events(source, target)?;
    copy_snapshots(source, target)?;
    let pending = report
        .counts
        .get("archive_commit_intents")
        .copied()
        .unwrap_or(0);
    if pending > 0 {
        report.warnings.push(format!(
            "{pending} pending commit intents were not imported; reconcile them with the old worker before cutover"
        ));
    }
    Ok(())
}

fn copy_libraries(source: &Connection, target: &Transaction<'_>) -> Result<()> {
    let mut query = source.prepare(
        "SELECT id,name,root_path,enabled,created_at,updated_at FROM libraries ORDER BY created_at,id",
    )?;
    for row in query.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, i64>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, String>(5)?,
        ))
    })? {
        let (id, name, root, enabled, created, updated) = row?;
        target.execute(
            "INSERT INTO libraries(id,name,root_path,enabled,created_at,updated_at) VALUES (?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET name=excluded.name,root_path=excluded.root_path,enabled=excluded.enabled,updated_at=excluded.updated_at",
            params![id, name, root, enabled, created, updated],
        )?;
    }
    Ok(())
}

fn copy_policies(source: &Connection, target: &Transaction<'_>) -> Result<()> {
    let mut query = source.prepare(
        "SELECT id,name,is_default,options_json,created_at,updated_at FROM conversion_profiles ORDER BY created_at,id",
    )?;
    for row in query.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, String>(5)?,
        ))
    })? {
        let (id, name, default, options, created, updated) = row?;
        target.execute(
            "INSERT INTO policies(id,name,is_default,policy_json,created_at,updated_at) VALUES (?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET name=excluded.name,is_default=excluded.is_default,policy_json=excluded.policy_json,updated_at=excluded.updated_at",
            params![id, name, default, serde_json::to_string(&legacy_policy(&options))?, created, updated],
        )?;
    }
    Ok(())
}

fn copy_schedules(source: &Connection, target: &Transaction<'_>) -> Result<()> {
    let mut query = source.prepare(
        "SELECT id,name,enabled,cadence,interval_value,interval_unit,weekday,day_of_month,hour,minute,library_id,profile_id,last_enqueued_at,next_run_at,created_at,updated_at FROM schedules ORDER BY created_at,id",
    )?;
    let rows = query.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, Option<i64>>(4)?,
            row.get::<_, Option<String>>(5)?,
            row.get::<_, Option<i64>>(6)?,
            row.get::<_, Option<i64>>(7)?,
            row.get::<_, i64>(8)?,
            row.get::<_, i64>(9)?,
            row.get::<_, String>(10)?,
            row.get::<_, String>(11)?,
            row.get::<_, Option<String>>(12)?,
            row.get::<_, Option<String>>(13)?,
            row.get::<_, String>(14)?,
            row.get::<_, String>(15)?,
        ))
    })?;
    for row in rows {
        let (
            id,
            name,
            enabled,
            cadence,
            interval,
            unit,
            weekday,
            month_day,
            hour,
            minute,
            library,
            policy,
            last,
            next,
            created,
            updated,
        ) = row?;
        let cadence_json = json!({"interval_value":interval,"interval_unit":unit,"weekday":weekday,"day_of_month":month_day,"hour":hour,"minute":minute});
        target.execute(
            "INSERT INTO schedules(id,name,enabled,cadence,cadence_json,library_id,policy_id,last_enqueued_at,next_run_at,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            params![id,name,enabled,cadence,cadence_json.to_string(),library,policy,last,next,created,updated],
        )?;
    }
    Ok(())
}

fn copy_jobs(source: &Connection, target: &Transaction<'_>) -> Result<()> {
    let mut query = source.prepare(
        "SELECT id,source,schedule_id,library_id,profile_id,status,options_json,queued_at,started_at,finished_at,archives_found,processed,skipped,failed,deleted,bytes_saved,bytes_increased,net_bytes_saved,error_message FROM jobs ORDER BY queued_at,id",
    )?;
    let mut rows = query.query([])?;
    while let Some(row) = rows.next()? {
        let options: String = row.get(6)?;
        let value: Value = serde_json::from_str(&options).unwrap_or_default();
        let status = terminal_status(row.get::<_, String>(5)?);
        target.execute(
            "INSERT INTO jobs(id,source,schedule_id,library_id,policy_id,status,policy_json,only_path,action_json,queued_at,started_at,finished_at,archives_found,processed,skipped,failed,deleted,bytes_saved,bytes_increased,net_bytes_saved,cancel_requested,error_message) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,?)",
            params![
                row.get::<_, String>(0)?, import_source(row.get::<_, String>(1)?), row.get::<_, Option<String>>(2)?,
                row.get::<_, String>(3)?, row.get::<_, Option<String>>(4)?, status,
                serde_json::to_string(&legacy_policy(&options))?, value.get("only_path").and_then(Value::as_str),
                legacy_repair(&value).map(|action| serde_json::to_string(&action)).transpose()?,
                row.get::<_, String>(7)?, row.get::<_, Option<String>>(8)?, row.get::<_, Option<String>>(9)?,
                row.get::<_, i64>(10)?, row.get::<_, i64>(11)?, row.get::<_, i64>(12)?, row.get::<_, i64>(13)?,
                row.get::<_, i64>(14)?, row.get::<_, i64>(15)?, row.get::<_, i64>(16)?, row.get::<_, i64>(17)?,
                row.get::<_, Option<String>>(18)?,
            ],
        )?;
    }
    Ok(())
}

fn copy_archives(source: &Connection, target: &Transaction<'_>) -> Result<()> {
    let mut query = source.prepare(
        "SELECT id,library_id,path,status,original_size,final_size,bytes_saved,percent_saved,archive_mtime,content_fingerprint,image_count,converted_pages,lossless_pages,lossy_pages,retained_pages,generation,conversion_mode,jxl_effort,processing_duration_seconds,error_message,review_status,review_note,reviewed_at,dismissed_until_rescan,created_at,updated_at FROM archives ORDER BY id",
    )?;
    let mut rows = query.query([])?;
    while let Some(row) = rows.next()? {
        let mtime = row
            .get::<_, Option<f64>>(8)?
            .map(|value| (value * 1_000_000_000.0) as i64);
        let duration = row
            .get::<_, Option<f64>>(18)?
            .map(|value| (value * 1000.0) as i64);
        target.execute(
            "INSERT INTO archives(id,library_id,path,status,baseline_size,current_size,bytes_saved,percent_saved,mtime_ns,fingerprint,image_count,converted_pages,lossless_pages,lossy_pages,retained_pages,generation,mode,effort,duration_ms,last_error,review_status,review_note,reviewed_at,dismissed_until_rescan,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            params![
                row.get::<_, i64>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?, row.get::<_, String>(3)?,
                row.get::<_, Option<i64>>(4)?, row.get::<_, Option<i64>>(5)?, row.get::<_, Option<i64>>(6)?.unwrap_or(0),
                row.get::<_, Option<f64>>(7)?.unwrap_or(0.0), mtime, row.get::<_, Option<String>>(9)?,
                row.get::<_, Option<i64>>(10)?.unwrap_or(0), row.get::<_, i64>(11)?, row.get::<_, i64>(12)?,
                row.get::<_, i64>(13)?, row.get::<_, i64>(14)?, row.get::<_, i64>(15)?, row.get::<_, Option<String>>(16)?,
                row.get::<_, Option<i64>>(17)?, duration, row.get::<_, Option<String>>(19)?, row.get::<_, String>(20)?,
                row.get::<_, Option<String>>(21)?, row.get::<_, Option<String>>(22)?, row.get::<_, i64>(23)?,
                row.get::<_, String>(24)?, row.get::<_, String>(25)?,
            ],
        )?;
    }
    Ok(())
}

fn copy_outcomes(source: &Connection, target: &Transaction<'_>) -> Result<()> {
    let mut query = source.prepare(
        "SELECT id,job_id,archive_id,path,status,original_size,final_size,bytes_saved,image_count,converted_pages,lossless_pages,lossy_pages,retained_pages,processing_duration_seconds,conversion_mode,jxl_effort,error_message,converted_at FROM archive_results ORDER BY id",
    )?;
    let mut rows = query.query([])?;
    while let Some(row) = rows.next()? {
        let status: String = row.get(4)?;
        let converted: i64 = row.get(9)?;
        let retained: i64 = row.get(12)?;
        let outcome = legacy_outcome(&status, converted, retained);
        let source_size = row.get::<_, Option<i64>>(5)?.unwrap_or(0);
        let final_size = row.get::<_, Option<i64>>(6)?.unwrap_or(source_size);
        target.execute(
            "INSERT INTO job_archive_outcomes(id,job_id,archive_id,path,outcome,source_size,final_size,bytes_saved,image_count,converted_pages,lossless_pages,lossy_pages,retained_pages,duration_ms,mode,effort,error_message,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            params![
                row.get::<_, i64>(0)?, row.get::<_, String>(1)?, row.get::<_, Option<i64>>(2)?, row.get::<_, String>(3)?,
                outcome, source_size, final_size, row.get::<_, Option<i64>>(7)?.unwrap_or(source_size-final_size),
                row.get::<_, Option<i64>>(8)?.unwrap_or(0), converted, row.get::<_, i64>(10)?, row.get::<_, i64>(11)?,
                retained, row.get::<_, Option<f64>>(13)?.map_or(0, |v| (v*1000.0) as i64),
                row.get::<_, Option<String>>(14)?, row.get::<_, Option<i64>>(15)?, row.get::<_, Option<String>>(16)?,
                row.get::<_, Option<String>>(17)?.unwrap_or_else(now),
            ],
        )?;
    }
    Ok(())
}

fn copy_pages(source: &Connection, target: &Transaction<'_>) -> Result<()> {
    let mut query = source.prepare(
        "SELECT job_id,path,entry_path,source_format,source_size,candidate_size,final_size,outcome,mode,distance,effort,reason,attempts_json,created_at FROM page_decisions ORDER BY id",
    )?;
    let mut rows = query.query([])?;
    while let Some(row) = rows.next()? {
        let job: String = row.get(0)?;
        let path: String = row.get(1)?;
        let outcome_id: Option<i64> = target
            .query_row(
                "SELECT id FROM job_archive_outcomes WHERE job_id=? AND path=?",
                params![job, path],
                |row| row.get(0),
            )
            .optional()?;
        let Some(outcome_id) = outcome_id else {
            continue;
        };
        target.execute(
            "INSERT INTO page_outcomes(job_outcome_id,entry_path,source_format,source_size,candidate_size,final_size,outcome,mode,distance,effort,reason,attempts_json,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            params![outcome_id,row.get::<_,String>(2)?,row.get::<_,Option<String>>(3)?,row.get::<_,Option<i64>>(4)?,row.get::<_,Option<i64>>(5)?,row.get::<_,Option<i64>>(6)?,row.get::<_,String>(7)?,row.get::<_,Option<String>>(8)?,row.get::<_,Option<f64>>(9)?,row.get::<_,Option<i64>>(10)?,row.get::<_,Option<String>>(11)?,row.get::<_,String>(12)?,row.get::<_,String>(13)?],
        )?;
    }
    Ok(())
}

fn copy_issues(source: &Connection, target: &Transaction<'_>) -> Result<()> {
    let mut query = source.prepare("SELECT id,archive_id,job_id,kind,stage,entry_path,related_entry_path,message,detail,status,remedy,created_at,updated_at,resolved_at FROM archive_issues ORDER BY id")?;
    let mut rows = query.query([])?;
    while let Some(row) = rows.next()? {
        target.execute("INSERT INTO issues(id,archive_id,job_id,kind,stage,entry_path,related_entry_path,message,detail,status,remedy,created_at,updated_at,resolved_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", params![row.get::<_,i64>(0)?,row.get::<_,i64>(1)?,row.get::<_,Option<String>>(2)?,row.get::<_,String>(3)?,row.get::<_,String>(4)?,row.get::<_,Option<String>>(5)?,row.get::<_,Option<String>>(6)?,row.get::<_,String>(7)?,row.get::<_,Option<String>>(8)?,row.get::<_,String>(9)?,row.get::<_,Option<String>>(10)?,row.get::<_,String>(11)?,row.get::<_,String>(12)?,row.get::<_,Option<String>>(13)?])?;
    }
    Ok(())
}

fn copy_reviews(source: &Connection, target: &Transaction<'_>) -> Result<()> {
    let mut query = source.prepare(
        "SELECT id,archive_id,job_id,action,note,created_at FROM review_actions ORDER BY id",
    )?;
    let mut rows = query.query([])?;
    while let Some(row) = rows.next()? {
        target.execute("INSERT INTO review_actions(id,archive_id,job_id,action,note,created_at) VALUES (?,?,?,?,?,?)", params![row.get::<_,i64>(0)?,row.get::<_,i64>(1)?,row.get::<_,Option<String>>(2)?,row.get::<_,String>(3)?,row.get::<_,Option<String>>(4)?,row.get::<_,String>(5)?])?;
    }
    Ok(())
}

fn copy_formats(source: &Connection, target: &Transaction<'_>) -> Result<()> {
    let mut query = source.prepare("SELECT archive_id,format,file_count,compressed_bytes,uncompressed_bytes,content_fingerprint,updated_at FROM archive_format_stats ORDER BY archive_id,format")?;
    let mut rows = query.query([])?;
    while let Some(row) = rows.next()? {
        target.execute("INSERT INTO archive_formats(archive_id,format,file_count,compressed_bytes,uncompressed_bytes,fingerprint,updated_at) VALUES (?,?,?,?,?,?,?)", params![row.get::<_,i64>(0)?,row.get::<_,String>(1)?,row.get::<_,i64>(2)?,row.get::<_,i64>(3)?,row.get::<_,i64>(4)?,row.get::<_,Option<String>>(5)?,row.get::<_,String>(6)?])?;
    }
    Ok(())
}

fn copy_events(source: &Connection, target: &Transaction<'_>) -> Result<()> {
    let mut query = source.prepare("SELECT id,job_id,created_at,level,event_type,message,archive_path,data_json FROM job_events ORDER BY id")?;
    let mut rows = query.query([])?;
    while let Some(row) = rows.next()? {
        target.execute("INSERT INTO job_events(id,job_id,created_at,level,event_type,message,archive_path,data_json) VALUES (?,?,?,?,?,?,?,?)", params![row.get::<_,i64>(0)?,row.get::<_,String>(1)?,row.get::<_,String>(2)?,row.get::<_,String>(3)?,row.get::<_,String>(4)?,row.get::<_,String>(5)?,row.get::<_,Option<String>>(6)?,row.get::<_,Option<String>>(7)?])?;
    }
    Ok(())
}

fn copy_snapshots(source: &Connection, target: &Transaction<'_>) -> Result<()> {
    let mut query = source.prepare("SELECT job_id,library_id,recorded_at,archive_count,optimized_count,failed_count,baseline_bytes,current_bytes,net_bytes,page_count,converted_pages,average_percent_saved FROM library_stat_snapshots ORDER BY recorded_at")?;
    let mut rows = query.query([])?;
    while let Some(row) = rows.next()? {
        target.execute("INSERT INTO library_snapshots(job_id,library_id,recorded_at,archive_count,optimized_count,failed_count,baseline_bytes,current_bytes,net_bytes,page_count,converted_pages,average_percent_saved) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", params![row.get::<_,String>(0)?,row.get::<_,String>(1)?,row.get::<_,String>(2)?,row.get::<_,i64>(3)?,row.get::<_,i64>(4)?,row.get::<_,i64>(5)?,row.get::<_,i64>(6)?,row.get::<_,i64>(7)?,row.get::<_,i64>(8)?,row.get::<_,i64>(9)?,row.get::<_,i64>(10)?,row.get::<_,f64>(11)?])?;
    }
    Ok(())
}

fn import_legacy(
    source: &Connection,
    target: &Transaction<'_>,
    report: &mut ImportReport,
) -> Result<()> {
    let now = now();
    let archive_select = legacy_select(
        source,
        "converted_archives",
        &[
            ("path", "NULL"),
            ("original_size", "NULL"),
            ("final_size", "NULL"),
            ("bytes_saved", "0"),
            ("percent_saved", "0"),
            ("converted_at", "NULL"),
            ("archive_mtime", "NULL"),
            ("status", "'processed'"),
            ("dominant_type", "NULL"),
            ("jxl_effort", "NULL"),
            ("processing_duration_seconds", "NULL"),
            ("image_count", "0"),
            ("jpg_count", "0"),
            ("png_count", "0"),
            ("error_message", "NULL"),
            ("conversion_mode", "NULL"),
            ("review_status", "'open'"),
            ("review_note", "NULL"),
            ("reviewed_at", "NULL"),
        ],
    )?;
    let mut archives = source.prepare(&format!(
        "SELECT {archive_select} FROM converted_archives ORDER BY path"
    ))?;
    let mut rows = archives.query([])?;
    while let Some(row) = rows.next()? {
        target.execute(
            "INSERT INTO archives(library_id,path,status,baseline_size,current_size,bytes_saved,percent_saved,mtime_ns,image_count,mode,effort,duration_ms,last_error,review_status,review_note,reviewed_at,created_at,updated_at) VALUES ('library-default',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            params![row.get::<_,String>(0)?,row.get::<_,Option<String>>(7)?.unwrap_or_else(||"processed".into()),row.get::<_,Option<i64>>(1)?,row.get::<_,Option<i64>>(2)?,row.get::<_,Option<i64>>(3)?.unwrap_or(0),row.get::<_,Option<f64>>(4)?.unwrap_or(0.0),row.get::<_,Option<f64>>(6)?.map(|v|(v*1_000_000_000.0) as i64),row.get::<_,Option<i64>>(11)?.unwrap_or(0),row.get::<_,Option<String>>(15)?,row.get::<_,Option<i64>>(9)?,row.get::<_,Option<f64>>(10)?.map(|v|(v*1000.0) as i64),row.get::<_,Option<String>>(14)?,row.get::<_,Option<String>>(16)?.unwrap_or_else(||"open".into()),row.get::<_,Option<String>>(17)?,row.get::<_,Option<String>>(18)?,now,now],
        )?;
    }
    if table(source, "runs")? {
        let run_select = legacy_select(
            source,
            "runs",
            &[
                ("id", "NULL"),
                ("started_at", "NULL"),
                ("finished_at", "NULL"),
                ("status", "'succeeded'"),
                ("archives_found", "0"),
                ("processed", "0"),
                ("skipped", "0"),
                ("failed", "0"),
                ("deleted", "0"),
                ("bytes_saved", "0"),
                ("bytes_increased", "0"),
                ("net_bytes_saved", "0"),
                ("error_message", "NULL"),
                ("jxl_effort", "10"),
                ("threads", "4"),
                ("smart_distance", "0"),
                ("distance", "NULL"),
            ],
        )?;
        let mut runs = source.prepare(&format!(
            "SELECT {run_select} FROM runs ORDER BY started_at,id"
        ))?;
        let mut rows = runs.query([])?;
        while let Some(row) = rows.next()? {
            let options = json!({"effort":row.get::<_,Option<i64>>(13)?.unwrap_or(10),"threads":row.get::<_,Option<i64>>(14)?.unwrap_or(4),"smart_distance":row.get::<_,Option<i64>>(15)?.unwrap_or(0)!=0,"distance":row.get::<_,Option<f64>>(16)?});
            target.execute("INSERT INTO jobs(id,source,library_id,status,policy_json,queued_at,started_at,finished_at,archives_found,processed,skipped,failed,deleted,bytes_saved,bytes_increased,net_bytes_saved,error_message) VALUES (?,'import','library-default',?,?,?,?,?,?,?,?,?,?,?,?,?,?)", params![row.get::<_,Option<String>>(0)?.unwrap_or_else(||Uuid::new_v4().to_string()),terminal_status(row.get::<_,Option<String>>(3)?.unwrap_or_else(||"succeeded".into())),serde_json::to_string(&legacy_policy(&options.to_string()))?,row.get::<_,Option<String>>(1)?.clone().unwrap_or_else(||now.clone()),row.get::<_,Option<String>>(1)?,row.get::<_,Option<String>>(2)?,row.get::<_,Option<i64>>(4)?.unwrap_or(0),row.get::<_,Option<i64>>(5)?.unwrap_or(0),row.get::<_,Option<i64>>(6)?.unwrap_or(0),row.get::<_,Option<i64>>(7)?.unwrap_or(0),row.get::<_,Option<i64>>(8)?.unwrap_or(0),row.get::<_,Option<i64>>(9)?.unwrap_or(0),row.get::<_,Option<i64>>(10)?.unwrap_or(0),row.get::<_,Option<i64>>(11)?.unwrap_or(0),row.get::<_,Option<String>>(12)?])?;
        }
    }
    if table(source, "archive_results")? {
        let result_select = legacy_select(
            source,
            "archive_results",
            &[
                ("run_id", "NULL"),
                ("path", "NULL"),
                ("status", "'processed'"),
                ("original_size", "0"),
                ("final_size", "0"),
                ("bytes_saved", "0"),
                ("percent_saved", "0"),
                ("dominant_type", "NULL"),
                ("jxl_effort", "NULL"),
                ("processing_duration_seconds", "0"),
                ("image_count", "0"),
                ("jpg_count", "0"),
                ("png_count", "0"),
                ("conversion_mode", "NULL"),
                ("error_message", "NULL"),
                ("converted_at", "NULL"),
            ],
        )?;
        let mut results = source.prepare(&format!(
            "SELECT {result_select} FROM archive_results ORDER BY converted_at,path"
        ))?;
        let mut rows = results.query([])?;
        while let Some(row) = rows.next()? {
            let Some(job_id) = row.get::<_, Option<String>>(0)? else {
                report
                    .warnings
                    .push("Skipped a legacy archive result without a run id".into());
                continue;
            };
            let Some(path) = row.get::<_, Option<String>>(1)? else {
                report
                    .warnings
                    .push("Skipped a legacy archive result without a path".into());
                continue;
            };
            if target
                .query_row("SELECT 1 FROM jobs WHERE id=?", [&job_id], |_| Ok(()))
                .optional()?
                .is_none()
            {
                target.execute("INSERT INTO jobs(id,source,library_id,status,policy_json,queued_at,finished_at) VALUES (?,'import','library-default','succeeded',?, ?, ?)", params![job_id, serde_json::to_string(&Policy::default())?, now, now])?;
                report
                    .warnings
                    .push(format!("Created a placeholder for legacy run {job_id}"));
            }
            let archive_id: Option<i64> = target
                .query_row(
                    "SELECT id FROM archives WHERE library_id='library-default' AND path=?",
                    [&path],
                    |row| row.get(0),
                )
                .optional()?;
            let status = row.get::<_, String>(2)?;
            let source_size = row.get::<_, i64>(3)?;
            let final_size = row.get::<_, i64>(4)?;
            target.execute(
                "INSERT OR IGNORE INTO job_archive_outcomes(job_id,archive_id,path,outcome,source_size,final_size,bytes_saved,image_count,duration_ms,mode,effort,error_message,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                params![job_id,archive_id,path,legacy_outcome(&status,0,0),source_size,final_size,row.get::<_,i64>(5)?,row.get::<_,i64>(10)?,(row.get::<_,f64>(9)?*1000.0) as i64,row.get::<_,Option<String>>(13)?,row.get::<_,Option<i64>>(8)?,row.get::<_,Option<String>>(14)?,row.get::<_,Option<String>>(15)?.unwrap_or_else(||now.clone())],
            )?;
        }
    }
    report.warnings.push("Legacy databases do not contain page decisions, issue history, schedules, or format inventory".into());
    Ok(())
}

fn legacy_select(source: &Connection, table_name: &str, fields: &[(&str, &str)]) -> Result<String> {
    let mut statement = source.prepare(&format!("PRAGMA table_info({table_name})"))?;
    let columns: HashSet<String> = statement
        .query_map([], |row| row.get(1))?
        .collect::<rusqlite::Result<_>>()?;
    Ok(fields
        .iter()
        .map(|(name, default)| {
            if columns.contains(*name) {
                (*name).to_owned()
            } else {
                format!("{default} AS {name}")
            }
        })
        .collect::<Vec<_>>()
        .join(","))
}

fn legacy_policy(options: &str) -> Policy {
    let value: Value = serde_json::from_str(options).unwrap_or_default();
    let strategy = if value
        .get("integrity_audit")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        Strategy::Audit
    } else if value
        .get("no_convert")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        Strategy::Recheck
    } else if value
        .get("smart_distance")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        Strategy::Smart
    } else if value.get("distance").is_some_and(|value| !value.is_null()) {
        Strategy::Lossy
    } else {
        Strategy::Lossless
    };
    let scope = if value
        .get("failed_only")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        Scope::Failed
    } else if value
        .get("recheck_all")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        Scope::Library
    } else {
        Scope::Unprocessed
    };
    Policy {
        strategy,
        scope,
        effort: value.get("effort").and_then(Value::as_u64).unwrap_or(10) as u8,
        threads: value.get("threads").and_then(Value::as_u64).unwrap_or(4) as usize,
        distance: value
            .get("distance")
            .and_then(Value::as_f64)
            .map(|value| value as f32),
        smart_mb: value
            .get("smart_mb")
            .and_then(Value::as_f64)
            .unwrap_or(10.0) as f32,
        smart_mp: value.get("smart_mp").and_then(Value::as_f64).unwrap_or(5.0) as f32,
        backup: value
            .get("backup")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    }
}

fn legacy_repair(value: &Value) -> Option<RepairAction> {
    let action = value.get("repair_action")?.as_str()?;
    let entry = || {
        value
            .get("repair_entry")
            .and_then(Value::as_str)
            .map(str::to_owned)
    };
    Some(match action {
        "replace" => RepairAction::Replace {
            entry: entry()?,
            source: value.get("repair_source")?.as_str()?.into(),
        },
        "delete" => RepairAction::Delete { entry: entry()? },
        "preserve" => RepairAction::Preserve { entry: entry()? },
        "recover_image" => RepairAction::Recover { entry: entry()? },
        "rebuild" => RepairAction::RebuildCrc {
            entries: value
                .get("repair_entries")
                .and_then(Value::as_array)
                .map(|entries| {
                    entries
                        .iter()
                        .filter_map(Value::as_str)
                        .map(str::to_owned)
                        .collect()
                })
                .unwrap_or_else(|| entry().into_iter().collect()),
        },
        "salvage" => RepairAction::Salvage { partial: false },
        "salvage_partial" => RepairAction::Salvage { partial: true },
        "repack" => RepairAction::Repack,
        "restore_original" => RepairAction::Restore { reconvert: false },
        "restore_reconvert" => RepairAction::Restore { reconvert: true },
        _ => return None,
    })
}

fn terminal_status(status: String) -> String {
    match status.as_str() {
        "queued" | "running" | "stopping" => "interrupted".into(),
        "finished" => "succeeded".into(),
        "finished_with_errors" => "partial".into(),
        _ => status,
    }
}

fn import_source(source: String) -> String {
    match source.as_str() {
        "manual" | "schedule" | "retry" => source,
        _ => "import".into(),
    }
}

fn legacy_outcome(status: &str, converted: i64, retained: i64) -> &'static str {
    if status == "failed" {
        "failed"
    } else if status == "audited" {
        "audited"
    } else if converted > 0 {
        "optimized"
    } else if retained > 0 {
        "retained"
    } else {
        "already_current"
    }
}
