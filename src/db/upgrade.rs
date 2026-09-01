use super::Database;
use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OpenFlags, OptionalExtension, backup::Backup};
use std::{
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
    time::Duration,
};
use tempfile::NamedTempFile;
use tracing::info;
use uuid::Uuid;

pub(super) fn legacy_in_place(database: &Database) -> Result<()> {
    if !requires_import(database.path())? {
        return Ok(());
    }

    let lock_path = sibling(database.path(), ".migration.lock");
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .with_context(|| format!("could not open migration lock {}", lock_path.display()))?;
    lock.lock()
        .with_context(|| format!("could not lock {}", lock_path.display()))?;

    // Another process may have completed the upgrade while this one waited.
    if !requires_import(database.path())? {
        return Ok(());
    }

    let backup = verified_backup(database.path())?;
    let parent = database.path().parent().unwrap_or_else(|| Path::new("."));
    let replacement = NamedTempFile::new_in(parent)?.into_temp_path();
    let replacement_database = Database::open(replacement.to_path_buf())?;
    replacement_database.migrate()?;
    replacement_database
        .import(&backup, false)
        .with_context(|| format!("could not import legacy database {}", backup.display()))?;
    prepare_for_replace(&replacement_database)?;

    remove_sidecar(database.path(), "-wal")?;
    remove_sidecar(database.path(), "-shm")?;
    std::fs::rename(&replacement, database.path()).with_context(|| {
        format!(
            "could not replace {} with upgraded database",
            database.path().display()
        )
    })?;
    sync(parent)?;
    info!(
        database = %database.path().display(),
        backup = %backup.display(),
        "imported existing database into the v5 schema"
    );
    Ok(())
}

fn requires_import(path: &Path) -> Result<bool> {
    if !path.is_file() || path.metadata()?.len() == 0 {
        return Ok(false);
    }
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    if table(&connection, "converted_archives")? {
        return Ok(true);
    }
    if !(table(&connection, "libraries")?
        && table(&connection, "jobs")?
        && table(&connection, "archives")?)
    {
        return Ok(false);
    }
    Ok(!(column(&connection, "archives", "fingerprint")?
        && column(&connection, "archives", "baseline_size")?
        && column(&connection, "jobs", "policy_json")?
        && table(&connection, "job_archive_outcomes")?))
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

fn column(connection: &Connection, table: &str, name: &str) -> Result<bool> {
    let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(columns.iter().any(|column| column == name))
}

fn verified_backup(source_path: &Path) -> Result<PathBuf> {
    let backup_dir = source_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("backups");
    std::fs::create_dir_all(&backup_dir)?;
    let stem = source_path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("cbzxl");
    let backup_path = backup_dir.join(format!(
        "{stem}.pre-v5-{}-{}.sqlite3",
        std::process::id(),
        Uuid::new_v4()
    ));
    let temporary = NamedTempFile::new_in(&backup_dir)?;
    let source = Connection::open_with_flags(
        source_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    let mut destination = Connection::open(temporary.path())?;
    Backup::new(&source, &mut destination)?.run_to_completion(
        256,
        Duration::from_millis(10),
        None,
    )?;
    let integrity: String = destination.query_row("PRAGMA quick_check", [], |row| row.get(0))?;
    ensure!(
        integrity == "ok",
        "pre-upgrade backup failed quick_check: {integrity}"
    );
    drop(destination);
    temporary.as_file().sync_all()?;
    temporary
        .persist(&backup_path)
        .map_err(|error| error.error)
        .with_context(|| format!("could not persist backup {}", backup_path.display()))?;
    let keep = std::env::var("MIGRATION_BACKUP_KEEP")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(3)
        .max(1);
    let mut backups = std::fs::read_dir(&backup_dir)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with(&format!("{stem}.pre-v"))
        })
        .collect::<Vec<_>>();
    backups.sort_by_key(|entry| {
        std::cmp::Reverse(entry.metadata().and_then(|value| value.modified()).ok())
    });
    for stale in backups.into_iter().skip(keep) {
        std::fs::remove_file(stale.path())?;
    }
    sync(&backup_dir)?;
    Ok(backup_path)
}

fn prepare_for_replace(database: &Database) -> Result<()> {
    let connection = database.connect()?;
    let integrity: String = connection.query_row("PRAGMA quick_check", [], |row| row.get(0))?;
    ensure!(
        integrity == "ok",
        "upgraded database failed quick_check: {integrity}"
    );
    connection.execute_batch("PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode=DELETE;")?;
    drop(connection);
    File::open(database.path())?.sync_all()?;
    Ok(())
}

fn sibling(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_owned();
    value.push(suffix);
    PathBuf::from(value)
}

fn remove_sidecar(path: &Path, suffix: &str) -> Result<()> {
    let sidecar = sibling(path, suffix);
    match std::fs::remove_file(&sidecar) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("could not remove {}", sidecar.display())),
    }
}

fn sync(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}
