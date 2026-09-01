use anyhow::Result;
use cbzxl::{
    db::Database,
    domain::{ArchiveResult, Outcome, RecordedArchive},
    schedule::{Cadence, IntervalUnit},
};

#[test]
fn due_schedule_is_enqueued_once_and_advanced() -> Result<()> {
    let root = tempfile::tempdir()?;
    let database = Database::open(root.path().join("cbzxl.sqlite3"))?;
    database.migrate()?;
    let cadence = Cadence::Interval {
        value: 6,
        unit: IntervalUnit::Hours,
    };
    database.connect()?.execute(
        "INSERT INTO schedules(id,name,enabled,cadence,cadence_json,library_id,policy_id,next_run_at,created_at,updated_at) VALUES ('schedule','Maintenance',1,'interval',?,'library-default','profile-balanced','2020-01-01T00:00:00Z','2020-01-01T00:00:00Z','2020-01-01T00:00:00Z')",
        [serde_json::to_string(&cadence)?],
    )?;
    assert_eq!(database.enqueue_due_schedules("UTC")?, 1);
    assert_eq!(database.enqueue_due_schedules("UTC")?, 0);
    let connection = database.connect()?;
    let (jobs,next):(i64,String)=connection.query_row(
        "SELECT (SELECT COUNT(*) FROM jobs WHERE schedule_id='schedule'),next_run_at FROM schedules WHERE id='schedule'",
        [],
        |row|Ok((row.get(0)?,row.get(1)?)),
    )?;
    assert_eq!(jobs, 1);
    assert!(next > chrono::Utc::now().to_rfc3339());
    Ok(())
}

#[test]
fn reconciliation_writes_only_actual_moves_and_missing_records() -> Result<()> {
    let root = tempfile::tempdir()?;
    let database = Database::open(root.path().join("cbzxl.sqlite3"))?;
    database.migrate()?;
    let connection = database.connect()?;
    let now = "2026-08-23T00:00:00Z";
    for (path, fingerprint) in [
        ("unchanged.cbz", "unchanged"),
        ("old-name.cbz", "moved"),
        ("gone.cbz", "missing"),
    ] {
        connection.execute(
            "INSERT INTO archives(library_id,path,status,current_size,fingerprint,created_at,updated_at) VALUES ('library-default',?,'processed',100,?,?,?)",
            rusqlite::params![path, fingerprint, now, now],
        )?;
    }
    let states = database.archive_states("library-default")?;
    let moved = states["old-name.cbz"].id;
    let missing = states["gone.cbz"].id;
    database.apply_reconciliation(
        "job-does-not-exist",
        "library-default",
        &[(moved, "new-name.cbz".into())],
        &[missing],
    )?;

    let connection = database.connect()?;
    let unchanged: (String, String) = connection.query_row(
        "SELECT status,updated_at FROM archives WHERE path='unchanged.cbz'",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    assert_eq!(unchanged, ("processed".into(), now.into()));
    assert_eq!(
        connection.query_row(
            "SELECT status FROM archives WHERE path='new-name.cbz'",
            [],
            |row| row.get::<_, String>(0),
        )?,
        "processed"
    );
    assert_eq!(
        connection.query_row(
            "SELECT status FROM archives WHERE path='gone.cbz'",
            [],
            |row| row.get::<_, String>(0),
        )?,
        "missing"
    );
    Ok(())
}

#[test]
fn migration_configures_persistent_wal_mode() -> Result<()> {
    let root = tempfile::tempdir()?;
    let database = Database::open(root.path().join("cbzxl.sqlite3"))?;
    database.migrate()?;
    let mode: String = database
        .connect()?
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
    assert_eq!(mode, "wal");
    Ok(())
}

#[test]
fn unchanged_results_update_only_job_totals_and_preserve_attention() -> Result<()> {
    let root = tempfile::tempdir()?;
    let database = Database::open(root.path().join("cbzxl.sqlite3"))?;
    database.migrate()?;
    let job = database.enqueue(
        "library-default",
        Some("profile-balanced"),
        &cbzxl::domain::Policy::default(),
        cbzxl::domain::JobSource::Manual,
        None,
    )?;
    let connection = database.connect()?;
    let timestamp = "2026-08-23T00:00:00Z";
    connection.execute(
        "INSERT INTO archives(library_id,path,status,current_size,mtime_ns,fingerprint,created_at,updated_at) VALUES ('library-default','Book.cbz','processed',100,42,'fingerprint',?,?)",
        rusqlite::params![timestamp, timestamp],
    )?;
    let archive_id = connection.last_insert_rowid();
    connection.execute(
        "INSERT INTO issues(archive_id,job_id,kind,stage,message,status,created_at,updated_at) VALUES (?,?,'archive_grew','verifying','Needs review','open',?,?)",
        rusqlite::params![archive_id, job, timestamp, timestamp],
    )?;
    database.record_batch(
        &job,
        "library-default",
        &[RecordedArchive {
            result: ArchiveResult {
                path: "Book.cbz".into(),
                outcome: Outcome::AlreadyCurrent,
                source_size: 100,
                final_size: 100,
                pages: 0,
                converted: 0,
                retained: 0,
                failed: 0,
                duration_ms: 0,
                fingerprint: Some("fingerprint".into()),
                error: None,
            },
            refresh_identity: false,
            mtime_ns: Some(42),
            input_fingerprint: Some("fingerprint".into()),
            final_fingerprint: Some("fingerprint".into()),
            mode: None,
            effort: None,
            pages: Vec::new(),
            issues: Vec::new(),
            formats: Vec::new(),
        }],
    )?;
    let connection = database.connect()?;
    assert_eq!(
        connection.query_row("SELECT skipped FROM jobs WHERE id=?", [&job], |row| row
            .get::<_, i64>(0))?,
        1
    );
    assert_eq!(
        connection.query_row(
            "SELECT COUNT(*) FROM job_archive_outcomes WHERE job_id=?",
            [&job],
            |row| row.get::<_, i64>(0)
        )?,
        0
    );
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
            "SELECT updated_at FROM archives WHERE id=?",
            [archive_id],
            |row| row.get::<_, String>(0)
        )?,
        timestamp
    );
    Ok(())
}

#[test]
fn full_rescan_reopens_dismissed_attention() -> Result<()> {
    let root = tempfile::tempdir()?;
    let database = Database::open(root.path().join("cbzxl.sqlite3"))?;
    database.migrate()?;
    let connection = database.connect()?;
    let timestamp = "2026-08-23T00:00:00Z";
    connection.execute(
        "INSERT INTO archives(library_id,path,status,current_size,review_status,dismissed_until_rescan,created_at,updated_at) VALUES ('library-default','Book.cbz','processed',100,'reviewed',1,?,?)",
        rusqlite::params![timestamp, timestamp],
    )?;
    let archive_id = connection.last_insert_rowid();
    connection.execute(
        "INSERT INTO issues(archive_id,kind,stage,message,status,remedy,created_at,updated_at) VALUES (?,'archive_grew','verifying','Review later','dismissed','dismiss',?,?)",
        rusqlite::params![archive_id, timestamp, timestamp],
    )?;
    assert_eq!(database.reopen_dismissed("library-default")?, 1);
    let connection = database.connect()?;
    assert_eq!(
        connection.query_row(
            "SELECT status FROM issues WHERE archive_id=?",
            [archive_id],
            |row| row.get::<_, String>(0)
        )?,
        "open"
    );
    let archive: (String, i64) = connection.query_row(
        "SELECT review_status,dismissed_until_rescan FROM archives WHERE id=?",
        [archive_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    assert_eq!(archive, ("open".into(), 0));
    Ok(())
}

#[test]
#[ignore = "opt-in 133k archive preparation benchmark"]
fn benchmark_in_memory_reconciliation_at_library_scale() -> Result<()> {
    use std::{collections::HashSet, time::Instant};

    const ARCHIVES: usize = 133_114;
    let root = tempfile::tempdir()?;
    let database = Database::open(root.path().join("cbzxl.sqlite3"))?;
    database.migrate()?;
    let mut connection = database.connect()?;
    let transaction = connection.transaction()?;
    {
        let mut insert = transaction.prepare(
            "INSERT INTO archives(library_id,path,status,current_size,created_at,updated_at) VALUES ('library-default',?,'processed',100,'2026-08-23T00:00:00Z','2026-08-23T00:00:00Z')",
        )?;
        for index in 0..ARCHIVES {
            insert.execute([format!("Series/{index:06}.cbz")])?;
        }
    }
    transaction.commit()?;

    let started = Instant::now();
    let states = database.archive_states("library-default")?;
    let loaded = started.elapsed();
    let discovered = states.keys().map(String::as_str).collect::<HashSet<_>>();
    let missing = states
        .iter()
        .filter(|(path, state)| state.status != "missing" && !discovered.contains(path.as_str()))
        .count();
    let reconciled = started.elapsed();

    let job = database.enqueue(
        "library-default",
        Some("profile-balanced"),
        &cbzxl::domain::Policy::default(),
        cbzxl::domain::JobSource::Manual,
        None,
    )?;
    let records = (0..10_000)
        .map(|index| RecordedArchive {
            result: ArchiveResult {
                path: format!("Series/{index:06}.cbz"),
                outcome: Outcome::AlreadyCurrent,
                source_size: 100,
                final_size: 100,
                pages: 0,
                converted: 0,
                retained: 0,
                failed: 0,
                duration_ms: 0,
                fingerprint: None,
                error: None,
            },
            refresh_identity: false,
            mtime_ns: None,
            input_fingerprint: None,
            final_fingerprint: None,
            mode: None,
            effort: None,
            pages: Vec::new(),
            issues: Vec::new(),
            formats: Vec::new(),
        })
        .collect::<Vec<_>>();
    let batch_started = Instant::now();
    database.record_batch(&job, "library-default", &records)?;
    let batch_elapsed = batch_started.elapsed();

    assert_eq!(states.len(), ARCHIVES);
    assert_eq!(missing, 0);
    eprintln!(
        "{ARCHIVES} archive states loaded in {loaded:?}; full no-change reconciliation in {reconciled:?}; 10,000 aggregate-only skips in {batch_elapsed:?}"
    );
    Ok(())
}
