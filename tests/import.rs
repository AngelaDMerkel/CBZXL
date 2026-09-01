use anyhow::Result;
use cbzxl::db::Database;
use rusqlite::Connection;

#[test]
fn legacy_import_is_complete_and_idempotent() -> Result<()> {
    let root = tempfile::tempdir()?;
    let source = root.path().join("legacy.sqlite3");
    Connection::open(&source)?.execute_batch(
        "
        CREATE TABLE converted_archives(
            path TEXT PRIMARY KEY, original_size INTEGER, final_size INTEGER,
            bytes_saved INTEGER, percent_saved REAL, converted_at TEXT,
            archive_mtime REAL, status TEXT, dominant_type TEXT, conversion_mode TEXT
        );
        CREATE TABLE runs(
            id TEXT PRIMARY KEY, started_at TEXT, finished_at TEXT, status TEXT,
            archives_found INTEGER, processed INTEGER, skipped INTEGER, failed INTEGER,
            bytes_saved INTEGER, bytes_increased INTEGER, net_bytes_saved INTEGER
        );
        CREATE TABLE archive_results(
            run_id TEXT, path TEXT, status TEXT, bytes_saved INTEGER,
            original_size INTEGER, final_size INTEGER, converted_at TEXT
        );
        INSERT INTO converted_archives VALUES(
            'Series/Book.cbz',1000,700,300,30,'2025-01-01T00:00:00Z',1,
            'processed','JPG','Lossless'
        );
        INSERT INTO runs VALUES(
            'legacy-run','2025-01-01T00:00:00Z','2025-01-01T00:01:00Z',
            'finished',1,1,0,0,300,0,300
        );
        INSERT INTO archive_results VALUES(
            'legacy-run','Series/Book.cbz','processed',300,1000,700,
            '2025-01-01T00:01:00Z'
        );
        ",
    )?;
    let database = Database::open(root.path().join("new.sqlite3"))?;
    database.migrate()?;

    let preview = database.import(&source, true)?;
    assert!(preview.dry_run);
    assert_eq!(preview.counts["converted_archives"], 1);
    let imported = database.import(&source, false)?;
    assert!(!imported.already_imported);
    assert!(database.import(&source, false)?.already_imported);

    let connection = database.connect()?;
    let counts: (i64, i64, i64) = connection.query_row(
        "SELECT (SELECT COUNT(*) FROM archives),(SELECT COUNT(*) FROM jobs),(SELECT COUNT(*) FROM job_archive_outcomes)",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    assert_eq!(counts, (1, 1, 1));
    assert_eq!(database.doctor()?.foreign_key_errors, 0);
    let exported = root.path().join("export.sqlite3");
    let (_temporary, mut reader, _) = database.export_file()?;
    std::io::copy(&mut reader, &mut std::fs::File::create(&exported)?)?;
    assert_eq!(
        Connection::open(exported)?
            .query_row("PRAGMA quick_check", [], |row| row.get::<_, String>(0))?,
        "ok"
    );
    Ok(())
}

#[test]
fn current_v3_import_preserves_normalized_history() -> Result<()> {
    let root = tempfile::tempdir()?;
    let source = root.path().join("current.sqlite3");
    Connection::open(&source)?.execute_batch(include_str!("fixtures/current_v3.sql"))?;
    let database = Database::open(root.path().join("new.sqlite3"))?;
    database.migrate()?;

    let report = database.import(&source, false)?;
    assert_eq!(report.counts["archives"], 1);
    assert!(report.warnings.is_empty());
    let connection = database.connect()?;
    let counts: (i64, i64, i64, i64, i64) = connection.query_row(
        "SELECT (SELECT COUNT(*) FROM archives),(SELECT COUNT(*) FROM jobs),(SELECT COUNT(*) FROM job_archive_outcomes),(SELECT COUNT(*) FROM page_outcomes),(SELECT COUNT(*) FROM issues)",
        [],
        |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?)),
    )?;
    assert_eq!(counts, (1, 1, 1, 1, 1));
    assert_eq!(database.doctor()?.foreign_key_errors, 0);
    Ok(())
}
