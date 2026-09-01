use anyhow::Result;
use cbzxl::db::Database;
use rusqlite::Connection;

#[test]
fn startup_upgrades_current_v3_in_place_once() -> Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("cbzxl-v3.sqlite3");
    Connection::open(&path)?.execute_batch(include_str!("fixtures/current_v3.sql"))?;

    let database = Database::open(path.clone())?;
    database.migrate()?;
    database.migrate()?;

    let connection = database.connect()?;
    assert_eq!(
        connection.query_row("SELECT fingerprint FROM archives", [], |row| {
            row.get::<_, String>(0)
        })?,
        "fingerprint"
    );
    assert_eq!(
        connection.query_row("SELECT COUNT(*) FROM job_archive_outcomes", [], |row| {
            row.get::<_, i64>(0)
        })?,
        1
    );
    assert_eq!(database.doctor()?.foreign_key_errors, 0);

    let backups =
        std::fs::read_dir(root.path().join("backups"))?.collect::<std::io::Result<Vec<_>>>()?;
    assert_eq!(backups.len(), 1);
    assert_eq!(
        Connection::open(backups[0].path())?
            .query_row("PRAGMA quick_check", [], |row| row.get::<_, String>(0))?,
        "ok"
    );
    Ok(())
}

#[test]
fn startup_upgrades_legacy_v2_in_place() -> Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("cbzxl-v2.sqlite3");
    Connection::open(&path)?.execute_batch(
        "
        CREATE TABLE converted_archives(
            path TEXT PRIMARY KEY, original_size INTEGER, final_size INTEGER,
            bytes_saved INTEGER, percent_saved REAL, converted_at TEXT,
            archive_mtime REAL, status TEXT, dominant_type TEXT, conversion_mode TEXT
        );
        INSERT INTO converted_archives VALUES(
            'Series/Book.cbz',1000,700,300,30,'2025-01-01T00:00:00Z',1,
            'processed','JPG','Lossless'
        );
        ",
    )?;

    let database = Database::open(path)?;
    database.migrate()?;
    let connection = database.connect()?;
    assert_eq!(
        connection.query_row("SELECT COUNT(*) FROM archives", [], |row| {
            row.get::<_, i64>(0)
        })?,
        1
    );
    assert_eq!(database.doctor()?.integrity, "ok");
    Ok(())
}
