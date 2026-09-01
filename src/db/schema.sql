PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS maintenance_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS libraries (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    root_path TEXT NOT NULL UNIQUE,
    enabled INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS policies (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    is_default INTEGER NOT NULL DEFAULT 0,
    policy_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS schedules (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    cadence TEXT NOT NULL,
    cadence_json TEXT NOT NULL,
    library_id TEXT NOT NULL REFERENCES libraries(id) ON DELETE RESTRICT,
    policy_id TEXT NOT NULL REFERENCES policies(id) ON DELETE RESTRICT,
    last_enqueued_at TEXT,
    next_run_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    schedule_id TEXT REFERENCES schedules(id) ON DELETE SET NULL,
    library_id TEXT NOT NULL REFERENCES libraries(id) ON DELETE RESTRICT,
    policy_id TEXT REFERENCES policies(id) ON DELETE SET NULL,
    status TEXT NOT NULL,
    policy_json TEXT NOT NULL,
    only_path TEXT,
    action_json TEXT,
    queued_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    archives_found INTEGER NOT NULL DEFAULT 0,
    processed INTEGER NOT NULL DEFAULT 0,
    skipped INTEGER NOT NULL DEFAULT 0,
    failed INTEGER NOT NULL DEFAULT 0,
    deleted INTEGER NOT NULL DEFAULT 0,
    bytes_saved INTEGER NOT NULL DEFAULT 0,
    bytes_increased INTEGER NOT NULL DEFAULT 0,
    net_bytes_saved INTEGER NOT NULL DEFAULT 0,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS archives (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    library_id TEXT NOT NULL REFERENCES libraries(id) ON DELETE CASCADE,
    path TEXT NOT NULL,
    status TEXT NOT NULL,
    baseline_size INTEGER,
    current_size INTEGER,
    bytes_saved INTEGER NOT NULL DEFAULT 0,
    percent_saved REAL NOT NULL DEFAULT 0,
    mtime_ns INTEGER,
    fingerprint TEXT,
    image_count INTEGER NOT NULL DEFAULT 0,
    converted_pages INTEGER NOT NULL DEFAULT 0,
    lossless_pages INTEGER NOT NULL DEFAULT 0,
    lossy_pages INTEGER NOT NULL DEFAULT 0,
    retained_pages INTEGER NOT NULL DEFAULT 0,
    generation INTEGER NOT NULL DEFAULT 1,
    mode TEXT,
    effort INTEGER,
    duration_ms INTEGER,
    last_error TEXT,
    review_status TEXT NOT NULL DEFAULT 'open',
    review_note TEXT,
    reviewed_at TEXT,
    dismissed_until_rescan INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(library_id, path)
);

CREATE TABLE IF NOT EXISTS job_archive_outcomes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    archive_id INTEGER REFERENCES archives(id) ON DELETE SET NULL,
    path TEXT NOT NULL,
    outcome TEXT NOT NULL,
    source_size INTEGER NOT NULL DEFAULT 0,
    final_size INTEGER NOT NULL DEFAULT 0,
    bytes_saved INTEGER NOT NULL DEFAULT 0,
    image_count INTEGER NOT NULL DEFAULT 0,
    converted_pages INTEGER NOT NULL DEFAULT 0,
    lossless_pages INTEGER NOT NULL DEFAULT 0,
    lossy_pages INTEGER NOT NULL DEFAULT 0,
    retained_pages INTEGER NOT NULL DEFAULT 0,
    failed_pages INTEGER NOT NULL DEFAULT 0,
    duration_ms INTEGER NOT NULL DEFAULT 0,
    mode TEXT,
    effort INTEGER,
    detail TEXT,
    error_message TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(job_id, path)
);

CREATE TABLE IF NOT EXISTS page_outcomes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_outcome_id INTEGER NOT NULL REFERENCES job_archive_outcomes(id) ON DELETE CASCADE,
    entry_path TEXT NOT NULL,
    source_format TEXT,
    source_size INTEGER,
    candidate_size INTEGER,
    final_size INTEGER,
    outcome TEXT NOT NULL,
    mode TEXT,
    distance REAL,
    effort INTEGER,
    reason TEXT,
    attempts_json TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL,
    UNIQUE(job_outcome_id, entry_path)
);

CREATE TABLE IF NOT EXISTS issues (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    archive_id INTEGER NOT NULL REFERENCES archives(id) ON DELETE CASCADE,
    job_id TEXT REFERENCES jobs(id) ON DELETE SET NULL,
    kind TEXT NOT NULL,
    stage TEXT NOT NULL,
    entry_path TEXT,
    related_entry_path TEXT,
    message TEXT NOT NULL,
    detail TEXT,
    status TEXT NOT NULL DEFAULT 'open',
    remedy TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    resolved_at TEXT
);

CREATE TABLE IF NOT EXISTS review_actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    archive_id INTEGER NOT NULL REFERENCES archives(id) ON DELETE CASCADE,
    job_id TEXT REFERENCES jobs(id) ON DELETE SET NULL,
    action TEXT NOT NULL,
    note TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS archive_formats (
    archive_id INTEGER NOT NULL REFERENCES archives(id) ON DELETE CASCADE,
    format TEXT NOT NULL,
    file_count INTEGER NOT NULL DEFAULT 0,
    compressed_bytes INTEGER NOT NULL DEFAULT 0,
    uncompressed_bytes INTEGER NOT NULL DEFAULT 0,
    fingerprint TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (archive_id, format)
);

CREATE TABLE IF NOT EXISTS job_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    created_at TEXT NOT NULL,
    level TEXT NOT NULL,
    event_type TEXT NOT NULL,
    message TEXT NOT NULL,
    archive_path TEXT,
    data_json TEXT
);

CREATE TABLE IF NOT EXISTS library_snapshots (
    job_id TEXT PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
    library_id TEXT NOT NULL REFERENCES libraries(id) ON DELETE CASCADE,
    recorded_at TEXT NOT NULL,
    archive_count INTEGER NOT NULL DEFAULT 0,
    optimized_count INTEGER NOT NULL DEFAULT 0,
    failed_count INTEGER NOT NULL DEFAULT 0,
    baseline_bytes INTEGER NOT NULL DEFAULT 0,
    current_bytes INTEGER NOT NULL DEFAULT 0,
    net_bytes INTEGER NOT NULL DEFAULT 0,
    page_count INTEGER NOT NULL DEFAULT 0,
    converted_pages INTEGER NOT NULL DEFAULT 0,
    average_percent_saved REAL NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS commit_intents (
    library_id TEXT NOT NULL REFERENCES libraries(id) ON DELETE CASCADE,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    path TEXT NOT NULL,
    input_fingerprint TEXT NOT NULL,
    candidate_fingerprint TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (library_id, path)
);

CREATE TABLE IF NOT EXISTS imports (
    id TEXT PRIMARY KEY,
    source_path TEXT NOT NULL,
    source_kind TEXT NOT NULL,
    source_digest TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL,
    counts_json TEXT NOT NULL,
    warnings_json TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT
);

CREATE INDEX IF NOT EXISTS jobs_queue ON jobs(status, queued_at);
CREATE INDEX IF NOT EXISTS schedules_due ON schedules(enabled, next_run_at);
CREATE INDEX IF NOT EXISTS archives_status ON archives(library_id, status, updated_at DESC);
CREATE INDEX IF NOT EXISTS archives_review ON archives(review_status, updated_at DESC);
CREATE INDEX IF NOT EXISTS archives_path_search ON archives(library_id, path COLLATE NOCASE);
CREATE INDEX IF NOT EXISTS archives_fingerprint ON archives(library_id, fingerprint) WHERE fingerprint IS NOT NULL;
CREATE INDEX IF NOT EXISTS outcomes_job ON job_archive_outcomes(job_id, id DESC);
CREATE INDEX IF NOT EXISTS outcomes_archive ON job_archive_outcomes(archive_id, id DESC);
CREATE INDEX IF NOT EXISTS outcomes_kind_duration ON job_archive_outcomes(outcome, duration_ms);
CREATE INDEX IF NOT EXISTS pages_outcome ON page_outcomes(job_outcome_id, outcome);
CREATE INDEX IF NOT EXISTS issues_open ON issues(kind, stage, updated_at DESC)
    WHERE status IN ('open', 'queued');
CREATE INDEX IF NOT EXISTS events_job ON job_events(job_id, id DESC);
CREATE INDEX IF NOT EXISTS formats_summary ON archive_formats(format, archive_id);
CREATE INDEX IF NOT EXISTS snapshots_recorded ON library_snapshots(recorded_at, job_id);
CREATE UNIQUE INDEX IF NOT EXISTS issues_entry_unique ON issues(archive_id, entry_path)
    WHERE entry_path IS NOT NULL AND status IN ('open', 'queued');
CREATE UNIQUE INDEX IF NOT EXISTS issues_archive_unique ON issues(archive_id)
    WHERE entry_path IS NULL AND status IN ('open', 'queued');
