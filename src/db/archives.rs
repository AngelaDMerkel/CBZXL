use super::{Database, now};
use crate::domain::{ArchiveResult, Outcome, RecordedArchive};
use anyhow::Result;
use rusqlite::{OptionalExtension, Transaction, params};
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct ArchiveState {
    pub id: i64,
    pub status: String,
    pub current_size: Option<u64>,
    pub mtime_ns: Option<i64>,
    pub fingerprint: Option<String>,
}

#[derive(Clone, Debug)]
pub struct CommitIntent {
    pub library_id: String,
    pub job_id: String,
    pub path: String,
    pub input_fingerprint: String,
    pub candidate_fingerprint: String,
    pub payload: serde_json::Value,
}

impl Database {
    pub fn reopen_dismissed(&self, library_id: &str) -> Result<usize> {
        let mut connection = self.connect()?;
        let transaction = connection.transaction()?;
        let reopened = transaction.execute(
            "UPDATE issues SET status='open',remedy=NULL,resolved_at=NULL,updated_at=? WHERE status='dismissed' AND archive_id IN (SELECT id FROM archives WHERE library_id=? AND dismissed_until_rescan=1)",
            params![now(), library_id],
        )?;
        transaction.execute(
            "UPDATE archives SET review_status='open',review_note=NULL,reviewed_at=NULL,dismissed_until_rescan=0,updated_at=? WHERE library_id=? AND dismissed_until_rescan=1",
            params![now(), library_id],
        )?;
        transaction.commit()?;
        Ok(reopened)
    }

    pub fn archive_states(&self, library_id: &str) -> Result<HashMap<String, ArchiveState>> {
        let connection = self.connect()?;
        let mut statement = connection.prepare(
            "SELECT path,id,status,current_size,mtime_ns,fingerprint FROM archives WHERE library_id=?",
        )?;
        Ok(statement
            .query_map([library_id], |row| {
                Ok((
                    row.get(0)?,
                    ArchiveState {
                        id: row.get(1)?,
                        status: row.get(2)?,
                        current_size: row.get::<_, Option<i64>>(3)?.map(|v| v.max(0) as u64),
                        mtime_ns: row.get(4)?,
                        fingerprint: row.get(5)?,
                    },
                ))
            })?
            .collect::<rusqlite::Result<_>>()?)
    }

    pub fn apply_reconciliation(
        &self,
        job_id: &str,
        library_id: &str,
        moved: &[(i64, String)],
        missing: &[i64],
    ) -> Result<bool> {
        if moved.is_empty() && missing.is_empty() {
            return Ok(!self.cancellation_requested(job_id)?);
        }
        if !moved.is_empty() {
            let mut connection = self.connect()?;
            let transaction = connection.transaction()?;
            let timestamp = now();
            let mut update = transaction
                .prepare("UPDATE archives SET path=?,updated_at=? WHERE id=? AND library_id=?")?;
            for (id, path) in moved {
                update.execute(params![path, timestamp, id, library_id])?;
            }
            drop(update);
            transaction.commit()?;
        }
        for ids in missing.chunks(1_000) {
            if self.cancellation_requested(job_id)? {
                return Ok(false);
            }
            let mut connection = self.connect()?;
            let transaction = connection.transaction()?;
            let timestamp = now();
            let mut mark = transaction.prepare(
                "UPDATE archives SET status='missing',last_error='Archive is no longer present in the library',review_status='reviewed',updated_at=? WHERE id=? AND library_id=? AND status!='missing'",
            )?;
            let mut resolve = transaction.prepare(
                "UPDATE issues SET status='resolved',remedy='source_missing',resolved_at=?,updated_at=? WHERE status IN ('open','queued') AND archive_id=?",
            )?;
            for id in ids {
                mark.execute(params![timestamp, id, library_id])?;
                resolve.execute(params![timestamp, timestamp, id])?;
            }
            drop(mark);
            drop(resolve);
            transaction.commit()?;
        }
        Ok(!self.cancellation_requested(job_id)?)
    }

    pub fn pending_commit_intents(&self) -> Result<Vec<CommitIntent>> {
        let connection = self.connect()?;
        let mut statement = connection.prepare("SELECT library_id,job_id,path,input_fingerprint,candidate_fingerprint,payload_json FROM commit_intents ORDER BY created_at,path")?;
        Ok(statement
            .query_map([], |row| {
                let payload: String = row.get(5)?;
                Ok(CommitIntent {
                    library_id: row.get(0)?,
                    job_id: row.get(1)?,
                    path: row.get(2)?,
                    input_fingerprint: row.get(3)?,
                    candidate_fingerprint: row.get(4)?,
                    payload: serde_json::from_str(&payload).unwrap_or_default(),
                })
            })?
            .collect::<rusqlite::Result<_>>()?)
    }

    pub fn failed_paths(&self, library_id: &str) -> Result<Vec<String>> {
        let connection = self.connect()?;
        let mut statement = connection.prepare(
            "SELECT DISTINCT a.path FROM archives a LEFT JOIN issues i ON i.archive_id=a.id AND i.status IN ('open','queued') WHERE a.library_id=? AND (a.status='failed' OR i.id IS NOT NULL) ORDER BY a.path COLLATE NOCASE",
        )?;
        Ok(statement
            .query_map([library_id], |row| row.get(0))?
            .collect::<rusqlite::Result<_>>()?)
    }

    pub fn archive_state(&self, library_id: &str, path: &str) -> Result<Option<ArchiveState>> {
        let connection = self.connect()?;
        Ok(connection
            .query_row(
                "SELECT id,status,current_size,mtime_ns,fingerprint FROM archives WHERE library_id=? AND path=?",
                params![library_id, path],
                |row| {
                    Ok(ArchiveState {
                        id: row.get(0)?,
                        status: row.get(1)?,
                        current_size: row.get::<_, Option<i64>>(2)?.map(|v| v.max(0) as u64),
                        mtime_ns: row.get(3)?,
                        fingerprint: row.get(4)?,
                    })
                },
            )
            .optional()?)
    }

    pub fn record_batch(
        &self,
        job_id: &str,
        library_id: &str,
        records: &[RecordedArchive],
    ) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let mut connection = self.connect()?;
        let transaction = connection.transaction()?;
        let mut processed = 0_i64;
        let mut skipped = 0_i64;
        let mut failed = 0_i64;
        let mut deleted = 0_i64;
        let mut saved = 0_i64;
        let mut increased = 0_i64;

        for record in records {
            let result = &record.result;
            if matches!(result.outcome, Outcome::AlreadyCurrent) {
                let archive_id = if record.refresh_identity {
                    refresh_archive_identity(&transaction, library_id, record)?
                } else if record.formats.is_empty() {
                    None
                } else {
                    transaction
                        .query_row(
                            "SELECT id FROM archives WHERE library_id=? AND path=?",
                            params![library_id, record.result.path],
                            |row| row.get(0),
                        )
                        .optional()?
                };
                replace_formats(&transaction, archive_id, record)?;
                skipped += 1;
                continue;
            }
            if matches!(result.outcome, Outcome::Deferred) {
                skipped += 1;
                continue;
            }
            let archive_id = record_archive(&transaction, library_id, record)?;
            let outcome_id = upsert_outcome(&transaction, job_id, archive_id, result, record)?;
            replace_page_outcomes(&transaction, outcome_id, record)?;
            replace_issues(&transaction, job_id, archive_id, record)?;
            replace_formats(&transaction, archive_id, record)?;
            if matches!(
                result.outcome,
                Outcome::Optimized | Outcome::Corrected | Outcome::Repaired
            ) {
                transaction.execute(
                    "DELETE FROM commit_intents WHERE library_id=? AND path=?",
                    params![library_id, result.path],
                )?;
            }
            match result.outcome {
                Outcome::Failed => failed += 1,
                Outcome::Deleted => deleted += 1,
                _ if result.failed > 0 => failed += 1,
                _ => processed += 1,
            }
            let delta = result.bytes_saved();
            if delta >= 0 {
                saved += delta;
            } else {
                increased -= delta;
            }
        }
        transaction.execute(
            "UPDATE jobs SET processed=processed+?,skipped=skipped+?,failed=failed+?,deleted=deleted+?,bytes_saved=bytes_saved+?,bytes_increased=bytes_increased+?,net_bytes_saved=net_bytes_saved+? WHERE id=?",
            params![processed, skipped, failed, deleted, saved, increased, saved-increased, job_id],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn put_commit_intent(
        &self,
        job_id: &str,
        library_id: &str,
        path: &str,
        input_fingerprint: &str,
        candidate_fingerprint: &str,
        payload: &serde_json::Value,
    ) -> Result<()> {
        let connection = self.connect()?;
        connection.execute(
            "INSERT INTO commit_intents(library_id,job_id,path,input_fingerprint,candidate_fingerprint,payload_json,created_at) VALUES (?,?,?,?,?,?,?) ON CONFLICT(library_id,path) DO UPDATE SET job_id=excluded.job_id,input_fingerprint=excluded.input_fingerprint,candidate_fingerprint=excluded.candidate_fingerprint,payload_json=excluded.payload_json,created_at=excluded.created_at",
            params![library_id,job_id,path,input_fingerprint,candidate_fingerprint,payload.to_string(),now()],
        )?;
        Ok(())
    }

    pub fn clear_commit_intent(&self, library_id: &str, path: &str) -> Result<()> {
        self.connect()?.execute(
            "DELETE FROM commit_intents WHERE library_id=? AND path=?",
            params![library_id, path],
        )?;
        Ok(())
    }
}

fn refresh_archive_identity(
    transaction: &Transaction<'_>,
    library_id: &str,
    record: &RecordedArchive,
) -> Result<Option<i64>> {
    let fingerprint = record
        .final_fingerprint
        .as_ref()
        .or(record.input_fingerprint.as_ref());
    if fingerprint.is_some() || record.mtime_ns.is_some() {
        transaction.execute(
            "UPDATE archives SET mtime_ns=COALESCE(?,mtime_ns),fingerprint=COALESCE(?,fingerprint),updated_at=? WHERE library_id=? AND path=? AND ((? IS NOT NULL AND mtime_ns IS NOT ?) OR (? IS NOT NULL AND fingerprint IS NOT ?))",
            params![record.mtime_ns,fingerprint,now(),library_id,record.result.path,record.mtime_ns,record.mtime_ns,fingerprint,fingerprint],
        )?;
    }
    Ok(transaction
        .query_row(
            "SELECT id FROM archives WHERE library_id=? AND path=?",
            params![library_id, record.result.path],
            |row| row.get(0),
        )
        .optional()?)
}

fn record_archive(
    transaction: &Transaction<'_>,
    library_id: &str,
    record: &RecordedArchive,
) -> Result<Option<i64>> {
    let result = &record.result;
    if let Some(fingerprint) = record
        .input_fingerprint
        .as_ref()
        .or(record.final_fingerprint.as_ref())
    {
        transaction.execute(
            "UPDATE archives SET path=?,updated_at=? WHERE id=(SELECT id FROM archives WHERE library_id=? AND status='missing' AND fingerprint=? ORDER BY id LIMIT 1) AND NOT EXISTS(SELECT 1 FROM archives WHERE library_id=? AND path=?)",
            params![result.path,now(),library_id,fingerprint,library_id,result.path],
        )?;
    }
    let existing = transaction
        .query_row(
            "SELECT id,status,baseline_size,current_size,fingerprint,generation,image_count,converted_pages,lossless_pages,lossy_pages,retained_pages FROM archives WHERE library_id=? AND path=?",
            params![library_id, result.path],
            |row| Ok((row.get::<_,i64>(0)?,row.get::<_,String>(1)?,row.get::<_,Option<i64>>(2)?,row.get::<_,Option<i64>>(3)?,row.get::<_,Option<String>>(4)?,row.get::<_,i64>(5)?,row.get::<_,i64>(6)?,row.get::<_,i64>(7)?,row.get::<_,i64>(8)?,row.get::<_,i64>(9)?,row.get::<_,i64>(10)?)),
        )
        .optional()?;

    if matches!(result.outcome, Outcome::Deleted) {
        if let Some((id, ..)) = existing {
            transaction.execute("DELETE FROM archives WHERE id=?", [id])?;
        }
        return Ok(None);
    }

    if matches!(result.outcome, Outcome::AlreadyCurrent | Outcome::Deferred)
        && let Some((id, ..)) = existing
    {
        if matches!(result.outcome, Outcome::AlreadyCurrent) {
            transaction.execute(
                "UPDATE archives SET mtime_ns=COALESCE(?,mtime_ns),fingerprint=COALESCE(?,fingerprint),updated_at=? WHERE id=?",
                params![record.mtime_ns,record.final_fingerprint.as_ref().or(record.input_fingerprint.as_ref()),now(),id],
            )?;
        }
        return Ok(Some(id));
    }

    let now = now();
    let (id, baseline, generation, same_generation, old_status, old_current, old_pages) =
        match existing {
            Some((
                id,
                status,
                baseline,
                current,
                fingerprint,
                generation,
                images,
                converted,
                lossless,
                lossy,
                retained,
            )) => {
                let same_generation = record.input_fingerprint.is_none()
                    || fingerprint.is_none()
                    || record.input_fingerprint == fingerprint;
                (
                    Some(id),
                    if same_generation {
                        baseline.unwrap_or(result.source_size as i64)
                    } else {
                        result.source_size as i64
                    },
                    if same_generation {
                        generation
                    } else {
                        generation + 1
                    },
                    same_generation,
                    status,
                    current,
                    (images, converted, lossless, lossy, retained),
                )
            }
            None => (
                None,
                result.source_size as i64,
                1,
                false,
                String::new(),
                None,
                (0, 0, 0, 0, 0),
            ),
        };
    let preserve = matches!(result.outcome, Outcome::Failed)
        && old_status == "processed"
        && old_current == Some(result.final_size as i64);
    let status = if preserve {
        "processed"
    } else if matches!(result.outcome, Outcome::Failed) {
        "failed"
    } else if matches!(result.outcome, Outcome::Audited) {
        "audited"
    } else {
        "processed"
    };
    let final_size = if preserve {
        old_current.unwrap_or(result.final_size as i64)
    } else {
        result.final_size as i64
    };
    let new_lossless = record
        .pages
        .iter()
        .filter(|page| {
            page.outcome == "converted"
                && page
                    .mode
                    .as_deref()
                    .is_some_and(|mode| mode.contains("Lossless"))
        })
        .count() as i64;
    let new_lossy = record
        .pages
        .iter()
        .filter(|page| {
            page.outcome == "converted"
                && page
                    .mode
                    .as_deref()
                    .is_some_and(|mode| mode.contains("Lossy"))
        })
        .count() as i64;
    let (images, converted, lossless, lossy, retained) = if preserve {
        old_pages
    } else if same_generation {
        let images = result.pages as i64;
        let converted = (old_pages.1 + result.converted as i64).min(images);
        let lossless = (old_pages.2 + new_lossless).min(converted);
        let lossy = (old_pages.3 + new_lossy).min(converted);
        (
            images,
            converted,
            lossless,
            lossy,
            (images - converted).max(0),
        )
    } else {
        (
            result.pages as i64,
            result.converted as i64,
            new_lossless,
            new_lossy,
            result.retained as i64,
        )
    };
    transaction.execute(
        "INSERT INTO archives(id,library_id,path,status,baseline_size,current_size,bytes_saved,percent_saved,mtime_ns,fingerprint,image_count,converted_pages,lossless_pages,lossy_pages,retained_pages,generation,mode,effort,duration_ms,last_error,review_status,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(library_id,path) DO UPDATE SET status=excluded.status,baseline_size=excluded.baseline_size,current_size=excluded.current_size,bytes_saved=excluded.bytes_saved,percent_saved=excluded.percent_saved,mtime_ns=COALESCE(excluded.mtime_ns,archives.mtime_ns),fingerprint=COALESCE(excluded.fingerprint,archives.fingerprint),image_count=excluded.image_count,converted_pages=excluded.converted_pages,lossless_pages=excluded.lossless_pages,lossy_pages=excluded.lossy_pages,retained_pages=excluded.retained_pages,generation=excluded.generation,mode=excluded.mode,effort=excluded.effort,duration_ms=excluded.duration_ms,last_error=excluded.last_error,review_status=excluded.review_status,updated_at=excluded.updated_at",
        params![id,library_id,result.path,status,baseline,final_size,baseline-final_size,percent(baseline,final_size),record.mtime_ns,record.final_fingerprint.as_ref().or(record.input_fingerprint.as_ref()),images,converted,lossless,lossy,retained,generation,record.mode,record.effort,result.duration_ms as i64,result.error,if record.issues.is_empty(){"reviewed"}else{"open"},now,now],
    )?;
    Ok(Some(transaction.query_row(
        "SELECT id FROM archives WHERE library_id=? AND path=?",
        params![library_id, result.path],
        |row| row.get(0),
    )?))
}

fn upsert_outcome(
    transaction: &Transaction<'_>,
    job_id: &str,
    archive_id: Option<i64>,
    result: &ArchiveResult,
    record: &RecordedArchive,
) -> Result<i64> {
    transaction.execute(
        "INSERT INTO job_archive_outcomes(job_id,archive_id,path,outcome,source_size,final_size,bytes_saved,image_count,converted_pages,lossless_pages,lossy_pages,retained_pages,failed_pages,duration_ms,mode,effort,detail,error_message,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(job_id,path) DO UPDATE SET archive_id=excluded.archive_id,outcome=excluded.outcome,source_size=excluded.source_size,final_size=excluded.final_size,bytes_saved=excluded.bytes_saved,image_count=excluded.image_count,converted_pages=excluded.converted_pages,lossless_pages=excluded.lossless_pages,lossy_pages=excluded.lossy_pages,retained_pages=excluded.retained_pages,failed_pages=excluded.failed_pages,duration_ms=excluded.duration_ms,mode=excluded.mode,effort=excluded.effort,detail=excluded.detail,error_message=excluded.error_message,created_at=excluded.created_at",
        params![job_id,archive_id,result.path,super::encode(result.outcome),result.source_size as i64,result.final_size as i64,result.bytes_saved(),result.pages as i64,result.converted as i64,record.pages.iter().filter(|p|p.mode.as_deref().is_some_and(|m|m.contains("Lossless"))).count() as i64,record.pages.iter().filter(|p|p.mode.as_deref().is_some_and(|m|m.contains("Lossy"))).count() as i64,result.retained as i64,result.failed as i64,result.duration_ms as i64,record.mode,record.effort,Option::<String>::None,result.error,now()],
    )?;
    Ok(transaction.query_row(
        "SELECT id FROM job_archive_outcomes WHERE job_id=? AND path=?",
        params![job_id, result.path],
        |row| row.get(0),
    )?)
}

fn replace_page_outcomes(
    transaction: &Transaction<'_>,
    outcome_id: i64,
    record: &RecordedArchive,
) -> Result<()> {
    transaction.execute(
        "DELETE FROM page_outcomes WHERE job_outcome_id=?",
        [outcome_id],
    )?;
    let now = now();
    for page in &record.pages {
        transaction.execute("INSERT INTO page_outcomes(job_outcome_id,entry_path,source_format,source_size,candidate_size,final_size,outcome,mode,distance,effort,reason,attempts_json,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", params![outcome_id,page.entry_path,page.source_format,page.source_size.map(|v|v as i64),page.candidate_size.map(|v|v as i64),page.final_size.map(|v|v as i64),page.outcome,page.mode,page.distance,page.effort,page.reason,serde_json::to_string(&page.attempts)?,now])?;
    }
    Ok(())
}

fn replace_issues(
    transaction: &Transaction<'_>,
    job_id: &str,
    archive_id: Option<i64>,
    record: &RecordedArchive,
) -> Result<()> {
    let Some(archive_id) = archive_id else {
        return Ok(());
    };
    let now = now();
    transaction.execute("UPDATE issues SET status='resolved',resolved_at=?,updated_at=? WHERE archive_id=? AND status IN ('open','queued')", params![now,now,archive_id])?;
    for issue in &record.issues {
        transaction.execute("INSERT INTO issues(archive_id,job_id,kind,stage,entry_path,related_entry_path,message,detail,status,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?, 'open',?,?)", params![archive_id,job_id,issue.kind,issue.stage,issue.entry_path,issue.related_entry_path,issue.message,issue.detail,now,now])?;
    }
    if record.issues.is_empty()
        && record.mode.as_deref() != Some("Extension correction")
        && transaction.query_row(
            "SELECT bytes_saved<0 FROM archives WHERE id=?",
            [archive_id],
            |row| row.get::<_, i64>(0),
        )? != 0
    {
        transaction.execute("INSERT INTO issues(archive_id,job_id,kind,stage,message,status,created_at,updated_at) VALUES (?,?,'archive_grew','verifying','The verified archive remains larger than its lifetime baseline','open',?,?)",params![archive_id,job_id,now,now])?;
        transaction.execute(
            "UPDATE archives SET review_status='open' WHERE id=?",
            [archive_id],
        )?;
    }
    Ok(())
}

fn replace_formats(
    transaction: &Transaction<'_>,
    archive_id: Option<i64>,
    record: &RecordedArchive,
) -> Result<()> {
    let Some(archive_id) = archive_id else {
        return Ok(());
    };
    if record.formats.is_empty() {
        return Ok(());
    }
    transaction.execute(
        "DELETE FROM archive_formats WHERE archive_id=?",
        [archive_id],
    )?;
    let now = now();
    for format in &record.formats {
        transaction.execute(
            "INSERT INTO archive_formats(archive_id,format,file_count,compressed_bytes,uncompressed_bytes,fingerprint,updated_at) VALUES (?,?,?,?,?,?,?)",
            params![archive_id,format.format,format.files as i64,format.compressed_bytes as i64,format.uncompressed_bytes as i64,record.final_fingerprint,now],
        )?;
    }
    Ok(())
}

fn percent(baseline: i64, current: i64) -> f64 {
    if baseline > 0 {
        (baseline - current) as f64 / baseline as f64 * 100.0
    } else {
        0.0
    }
}
