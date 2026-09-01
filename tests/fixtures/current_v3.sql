CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY, applied_at TEXT);
WITH RECURSIVE versions(value) AS (VALUES(1) UNION ALL SELECT value+1 FROM versions WHERE value<15)
INSERT INTO schema_migrations SELECT value,'2026-01-01T00:00:00Z' FROM versions;

CREATE TABLE libraries(id TEXT PRIMARY KEY,name TEXT,root_path TEXT,enabled INTEGER,created_at TEXT,updated_at TEXT);
INSERT INTO libraries VALUES('library-default','Primary library','/old-library',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z');

CREATE TABLE conversion_profiles(id TEXT PRIMARY KEY,name TEXT,is_default INTEGER,options_json TEXT,created_at TEXT,updated_at TEXT);
INSERT INTO conversion_profiles VALUES('profile-balanced','Balanced lossless',1,'{"effort":10,"threads":4}','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z');

CREATE TABLE schedules(
 id TEXT PRIMARY KEY,name TEXT,enabled INTEGER,cadence TEXT,interval_value INTEGER,interval_unit TEXT,
 weekday INTEGER,day_of_month INTEGER,hour INTEGER,minute INTEGER,library_id TEXT,profile_id TEXT,
 last_enqueued_at TEXT,next_run_at TEXT,created_at TEXT,updated_at TEXT
);

CREATE TABLE jobs(
 id TEXT PRIMARY KEY,source TEXT,schedule_id TEXT,library_id TEXT,profile_id TEXT,status TEXT,options_json TEXT,
 queued_at TEXT,started_at TEXT,finished_at TEXT,archives_found INTEGER,processed INTEGER,skipped INTEGER,
 failed INTEGER,deleted INTEGER,bytes_saved INTEGER,bytes_increased INTEGER,net_bytes_saved INTEGER,error_message TEXT
);
INSERT INTO jobs VALUES('current-job','manual',NULL,'library-default','profile-balanced','succeeded','{"effort":10,"threads":4}',
 '2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','2026-01-01T00:01:00Z',1,1,0,0,0,300,0,300,NULL);

CREATE TABLE archives(
 id INTEGER PRIMARY KEY,library_id TEXT,path TEXT,status TEXT,original_size INTEGER,final_size INTEGER,bytes_saved INTEGER,
 percent_saved REAL,archive_mtime REAL,content_fingerprint TEXT,image_count INTEGER,converted_pages INTEGER,
 lossless_pages INTEGER,lossy_pages INTEGER,retained_pages INTEGER,generation INTEGER,conversion_mode TEXT,jxl_effort INTEGER,
 processing_duration_seconds REAL,error_message TEXT,review_status TEXT,review_note TEXT,reviewed_at TEXT,
 dismissed_until_rescan INTEGER,created_at TEXT,updated_at TEXT
);
INSERT INTO archives VALUES(1,'library-default','Series/Book.cbz','processed',1000,700,300,30,1,'fingerprint',1,1,1,0,0,1,
 'Lossless',10,1.2,NULL,'open',NULL,NULL,0,'2026-01-01T00:00:00Z','2026-01-01T00:01:00Z');

CREATE TABLE archive_results(
 id INTEGER PRIMARY KEY,job_id TEXT,archive_id INTEGER,path TEXT,status TEXT,original_size INTEGER,final_size INTEGER,
 bytes_saved INTEGER,image_count INTEGER,converted_pages INTEGER,lossless_pages INTEGER,lossy_pages INTEGER,
 retained_pages INTEGER,processing_duration_seconds REAL,conversion_mode TEXT,jxl_effort INTEGER,error_message TEXT,converted_at TEXT
);
INSERT INTO archive_results VALUES(1,'current-job',1,'Series/Book.cbz','processed',1000,700,300,1,1,1,0,0,1.2,'Lossless',10,NULL,'2026-01-01T00:01:00Z');

CREATE TABLE page_decisions(
 id INTEGER PRIMARY KEY,job_id TEXT,archive_id INTEGER,path TEXT,entry_path TEXT,source_format TEXT,source_size INTEGER,
 candidate_size INTEGER,final_size INTEGER,outcome TEXT,mode TEXT,distance REAL,effort INTEGER,reason TEXT,attempts_json TEXT,created_at TEXT
);
INSERT INTO page_decisions VALUES(1,'current-job',1,'Series/Book.cbz','001.jpg','image/jpeg',1000,700,700,'converted','Lossless',0,10,'verified','[]','2026-01-01T00:01:00Z');

CREATE TABLE archive_issues(
 id INTEGER PRIMARY KEY,archive_id INTEGER,job_id TEXT,kind TEXT,stage TEXT,entry_path TEXT,related_entry_path TEXT,
 message TEXT,detail TEXT,status TEXT,remedy TEXT,created_at TEXT,updated_at TEXT,resolved_at TEXT
);
INSERT INTO archive_issues VALUES(1,1,'current-job','page_encode_failed','encoding','002.jpg',NULL,'failure',NULL,'open',NULL,'2026-01-01T00:01:00Z','2026-01-01T00:01:00Z',NULL);

CREATE TABLE review_actions(id INTEGER PRIMARY KEY,archive_id INTEGER,job_id TEXT,action TEXT,note TEXT,created_at TEXT);
CREATE TABLE archive_format_stats(archive_id INTEGER,format TEXT,file_count INTEGER,compressed_bytes INTEGER,uncompressed_bytes INTEGER,content_fingerprint TEXT,updated_at TEXT);
INSERT INTO archive_format_stats VALUES(1,'JPEG XL',1,700,700,'fingerprint','2026-01-01T00:01:00Z');
CREATE TABLE job_events(id INTEGER PRIMARY KEY,job_id TEXT,created_at TEXT,level TEXT,event_type TEXT,message TEXT,archive_path TEXT,data_json TEXT);
CREATE TABLE library_stat_snapshots(job_id TEXT PRIMARY KEY,library_id TEXT,recorded_at TEXT,archive_count INTEGER,optimized_count INTEGER,failed_count INTEGER,baseline_bytes INTEGER,current_bytes INTEGER,net_bytes INTEGER,page_count INTEGER,converted_pages INTEGER,average_percent_saved REAL);
INSERT INTO library_stat_snapshots VALUES('current-job','library-default','2026-01-01T00:01:00Z',1,1,0,1000,700,300,1,1,30);
CREATE TABLE archive_commit_intents(id INTEGER PRIMARY KEY,library_id TEXT,job_id TEXT,path TEXT,input_fingerprint TEXT,candidate_fingerprint TEXT,payload_json TEXT,created_at TEXT);

