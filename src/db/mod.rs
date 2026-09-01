mod archives;
mod import;
mod jobs;
mod upgrade;

pub use archives::ArchiveState;

use crate::domain::{Job, JobSource, JobStatus, Library, Policy, RepairAction};
use anyhow::{Context, Result};
pub use import::ImportReport;
use rusqlite::{
    Connection, OpenFlags, OptionalExtension, TransactionBehavior, backup::Backup, params,
};
use serde::Serialize;
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

const SCHEMA: &str = include_str!("schema.sql");

#[derive(Clone, Debug)]
pub struct Database {
    path: PathBuf,
}

#[derive(Debug, Serialize)]
pub struct DoctorReport {
    pub database: String,
    pub integrity: String,
    pub foreign_key_errors: usize,
    pub sqlite_version: String,
    pub schema_version: i64,
}

impl Database {
    pub fn open(path: PathBuf) -> Result<Self> {
        Ok(Self { path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn connect(&self) -> Result<Connection> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("could not create {}", parent.display()))?;
        }
        let connection = Connection::open_with_flags(
            &self.path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.execute_batch(
            "PRAGMA foreign_keys=ON; PRAGMA synchronous=NORMAL; PRAGMA temp_store=MEMORY; PRAGMA mmap_size=268435456;",
        )?;
        Ok(connection)
    }

    pub fn migrate(&self) -> Result<()> {
        upgrade::legacy_in_place(self)?;
        let mut connection = self.connect()?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute_batch(SCHEMA)?;
        let now = now();
        transaction.execute(
            "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (1, ?)",
            [&now],
        )?;
        let has_action = transaction
            .prepare("PRAGMA table_info(jobs)")?
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<rusqlite::Result<Vec<_>>>()?
            .iter()
            .any(|column| column == "action_json");
        if !has_action {
            transaction.execute("ALTER TABLE jobs ADD COLUMN action_json TEXT", [])?;
        }
        transaction.execute(
            "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (2, ?)",
            [&now],
        )?;
        transaction.execute(
            "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (3, ?)",
            [&now],
        )?;
        transaction.execute(
            "INSERT OR IGNORE INTO libraries(id,name,root_path,enabled,created_at,updated_at) VALUES ('library-default','Primary library',?,1,?,?)",
            params![std::env::var("LIBRARY_PATH").unwrap_or_else(|_| "/data".into()), now, now],
        )?;
        for (id, name, default, policy) in builtins() {
            transaction.execute(
                "INSERT OR IGNORE INTO policies(id,name,is_default,policy_json,created_at,updated_at) VALUES (?,?,?,?,?,?)",
                params![id, name, default, serde_json::to_string(&policy)?, now, now],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn doctor(&self) -> Result<DoctorReport> {
        let connection = self.connect()?;
        let integrity = connection.query_row("PRAGMA quick_check", [], |row| row.get(0))?;
        let mut statement = connection.prepare("PRAGMA foreign_key_check")?;
        let mut rows = statement.query([])?;
        let mut foreign_key_errors = 0;
        while rows.next()?.is_some() {
            foreign_key_errors += 1;
        }
        let sqlite_version =
            connection.query_row("SELECT sqlite_version()", [], |row| row.get(0))?;
        let schema_version = connection.query_row(
            "SELECT COALESCE(MAX(version),0) FROM schema_migrations",
            [],
            |row| row.get(0),
        )?;
        Ok(DoctorReport {
            database: self.path.display().to_string(),
            integrity,
            foreign_key_errors,
            sqlite_version,
            schema_version,
        })
    }

    pub fn maintenance(&self, force: bool, maximum_age: Duration) -> Result<bool> {
        let connection = self.connect()?;
        connection.execute_batch(
            "CREATE INDEX IF NOT EXISTS pages_retention ON page_outcomes(created_at, id) WHERE outcome IN ('converted', 'retained', 'extension_corrected', 'unsupported')",
        )?;
        if !force {
            let recent: i64 = connection.query_row(
                "SELECT EXISTS(SELECT 1 FROM maintenance_state WHERE key='quick_check' AND julianday('now')-julianday(updated_at)<?)",
                [maximum_age.as_secs_f64() / 86_400.0],
                |row| row.get(0),
            )?;
            if recent != 0 {
                return Ok(false);
            }
        }
        let integrity: String = connection.query_row("PRAGMA quick_check", [], |row| row.get(0))?;
        anyhow::ensure!(
            integrity == "ok",
            "database quick_check failed: {integrity}"
        );
        connection.execute("INSERT INTO maintenance_state(key,value,updated_at) VALUES ('quick_check','ok',?) ON CONFLICT(key) DO UPDATE SET value='ok',updated_at=excluded.updated_at",[now()])?;
        connection.execute_batch("PRAGMA wal_checkpoint(PASSIVE)")?;
        connection.execute_batch("PRAGMA optimize")?;
        Ok(true)
    }

    pub fn prune_history(&self, retention_days: u64) -> Result<usize> {
        let connection = self.connect()?;
        let cutoff = (OffsetDateTime::now_utc()
            - time::Duration::days(retention_days.min(i64::MAX as u64) as i64))
        .format(&Rfc3339)?;
        Ok(connection.execute(
            "DELETE FROM page_outcomes WHERE id IN (SELECT p.id FROM page_outcomes p WHERE p.created_at<? AND p.outcome IN ('converted','retained','extension_corrected','unsupported') AND NOT EXISTS(SELECT 1 FROM job_archive_outcomes o JOIN issues i ON i.archive_id=o.archive_id WHERE o.id=p.job_outcome_id AND i.status IN ('open','queued')) ORDER BY p.id LIMIT 10000)",
            [cutoff],
        )?)
    }

    pub fn prune_transient_outcomes(&self) -> Result<usize> {
        Ok(self.connect()?.execute(
            "DELETE FROM job_archive_outcomes WHERE id IN (SELECT id FROM job_archive_outcomes WHERE outcome IN ('already_current','deferred') ORDER BY id LIMIT 10000)",
            [],
        )?)
    }

    pub fn export_file(&self) -> Result<(tempfile::NamedTempFile, std::fs::File, u64)> {
        let source = self.connect()?;
        let temporary = tempfile::NamedTempFile::new()?;
        let mut destination = Connection::open(temporary.path())?;
        Backup::new(&source, &mut destination)?.run_to_completion(
            256,
            Duration::from_millis(10),
            None,
        )?;
        drop(destination);
        let size = temporary.as_file().metadata()?.len();
        let reader = temporary.reopen()?;
        Ok((temporary, reader, size))
    }

    pub fn import(&self, source: &Path, dry_run: bool) -> Result<ImportReport> {
        import::run(self, source, dry_run)
    }

    pub fn libraries(&self) -> Result<Vec<Library>> {
        let connection = self.connect()?;
        let mut statement = connection
            .prepare("SELECT id,name,root_path,enabled FROM libraries ORDER BY name,id")?;
        Ok(statement
            .query_map([], |row| {
                Ok(Library {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    root: PathBuf::from(row.get::<_, String>(2)?),
                    enabled: row.get::<_, i64>(3)? != 0,
                })
            })?
            .collect::<rusqlite::Result<_>>()?)
    }

    pub fn enqueue(
        &self,
        library_id: &str,
        policy_id: Option<&str>,
        policy: &Policy,
        source: JobSource,
        only_path: Option<&str>,
    ) -> Result<String> {
        let connection = self.connect()?;
        let id = Uuid::new_v4().to_string();
        connection.execute(
            "INSERT INTO jobs(id,source,library_id,policy_id,status,policy_json,only_path,queued_at) VALUES (?,?,?,?,?,?,?,?)",
            params![id, encode(source), library_id, policy_id, "queued", serde_json::to_string(policy)?, only_path, now()],
        )?;
        Ok(id)
    }

    pub fn enqueue_repair(
        &self,
        library_id: &str,
        policy_id: Option<&str>,
        policy: &Policy,
        path: &str,
        action: &RepairAction,
    ) -> Result<String> {
        let connection = self.connect()?;
        let id = Uuid::new_v4().to_string();
        connection.execute(
            "INSERT INTO jobs(id,source,library_id,policy_id,status,policy_json,only_path,action_json,queued_at) VALUES (?,'retry',?,?,'queued',?,?,?,?)",
            params![id, library_id, policy_id, serde_json::to_string(policy)?, path, serde_json::to_string(action)?, now()],
        )?;
        Ok(id)
    }

    pub fn claim(&self) -> Result<Option<Job>> {
        let mut connection = self.connect()?;
        let queued: i64 = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM jobs WHERE status='queued')",
            [],
            |row| row.get(0),
        )?;
        if queued == 0 {
            return Ok(None);
        }
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let row = transaction
            .query_row(
                "SELECT id,library_id,policy_id,source,status,policy_json,only_path,action_json,queued_at,cancel_requested FROM jobs WHERE status='queued' ORDER BY queued_at,id LIMIT 1",
                [],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, Option<String>>(2)?, row.get::<_, String>(3)?, row.get::<_, String>(4)?, row.get::<_, String>(5)?, row.get::<_, Option<String>>(6)?, row.get::<_, Option<String>>(7)?, row.get::<_, String>(8)?, row.get::<_, i64>(9)?)),
            )
            .optional()?;
        let Some((
            id,
            library_id,
            policy_id,
            source,
            status,
            policy,
            only_path,
            action,
            queued_at,
            cancel,
        )) = row
        else {
            return Ok(None);
        };
        let changed = transaction.execute(
            "UPDATE jobs SET status='running',started_at=?,cancel_requested=0 WHERE id=? AND status='queued'",
            params![now(), id],
        )?;
        if changed != 1 {
            return Ok(None);
        }
        transaction.commit()?;
        Ok(Some(Job {
            id,
            library_id,
            policy_id,
            source: decode_source(&source),
            status: decode_status(&status),
            policy: serde_json::from_str(&policy)?,
            only_path,
            action: action
                .map(|value| serde_json::from_str(&value))
                .transpose()?,
            queued_at: OffsetDateTime::parse(&queued_at, &Rfc3339)?,
            cancel_requested: cancel != 0,
        }))
    }
}

pub(crate) fn now() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .expect("RFC 3339 formatting")
}

pub(crate) fn encode<T: Serialize>(value: T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_default()
}

fn decode_source(value: &str) -> JobSource {
    serde_json::from_value(serde_json::Value::String(value.into())).unwrap_or(JobSource::Import)
}

fn decode_status(value: &str) -> JobStatus {
    serde_json::from_value(serde_json::Value::String(value.into())).unwrap_or(JobStatus::Failed)
}

fn builtins() -> Vec<(&'static str, &'static str, i64, Policy)> {
    use crate::domain::{Scope, Strategy};
    vec![
        (
            "profile-balanced",
            "Balanced lossless",
            1,
            Policy::default(),
        ),
        (
            "profile-smart",
            "Smart lossy fallback",
            0,
            Policy {
                strategy: Strategy::Smart,
                distance: Some(0.5),
                ..Policy::default()
            },
        ),
        (
            "profile-recheck",
            "Recheck without conversion",
            0,
            Policy {
                strategy: Strategy::Recheck,
                ..Policy::default()
            },
        ),
        (
            "profile-integrity",
            "Integrity audit",
            0,
            Policy {
                strategy: Strategy::Audit,
                scope: Scope::Library,
                ..Policy::default()
            },
        ),
    ]
}
