use super::{Database, now};
use crate::{
    domain::{JobStatus, Policy},
    schedule::Cadence,
};
use anyhow::Result;
use rusqlite::{OptionalExtension, params};
use uuid::Uuid;

impl Database {
    pub fn add_event(
        &self,
        job_id: &str,
        level: &str,
        event_type: &str,
        message: &str,
        archive: Option<&str>,
        data: Option<&serde_json::Value>,
    ) -> Result<()> {
        self.connect()?.execute(
            "INSERT INTO job_events(job_id,created_at,level,event_type,message,archive_path,data_json) VALUES (?,?,?,?,?,?,?)",
            params![job_id,now(),level,event_type,message,archive,data.map(serde_json::Value::to_string)],
        )?;
        Ok(())
    }

    pub fn enqueue_due_schedules(&self, timezone: &str) -> Result<usize> {
        let mut connection = self.connect()?;
        let due: i64 = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM schedules WHERE enabled=1 AND next_run_at IS NOT NULL AND next_run_at<=?)",
            [now()],
            |row| row.get(0),
        )?;
        if due == 0 {
            return Ok(0);
        }
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let now = now();
        let schedules: Vec<(String, String, String, String)> = {
            let mut statement = transaction.prepare(
                "SELECT s.id,s.library_id,s.policy_id,s.cadence_json FROM schedules s WHERE s.enabled=1 AND s.next_run_at IS NOT NULL AND s.next_run_at<=? ORDER BY s.next_run_at,s.id",
            )?;
            statement
                .query_map([&now], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                })?
                .collect::<rusqlite::Result<_>>()?
        };
        for (schedule_id, library_id, policy_id, cadence_json) in &schedules {
            let policy_json: String = transaction.query_row(
                "SELECT policy_json FROM policies WHERE id=?",
                [policy_id],
                |row| row.get(0),
            )?;
            let policy: Policy = serde_json::from_str(&policy_json)?;
            let cadence: Cadence = serde_json::from_str(cadence_json)?;
            transaction.execute(
                "INSERT INTO jobs(id,source,schedule_id,library_id,policy_id,status,policy_json,queued_at) VALUES (?,'schedule',?,?,?,'queued',?,?)",
                params![Uuid::new_v4().to_string(),schedule_id,library_id,policy_id,serde_json::to_string(&policy)?,now],
            )?;
            transaction.execute(
                "UPDATE schedules SET last_enqueued_at=?,next_run_at=?,updated_at=? WHERE id=?",
                params![now, cadence.next(timezone)?, now, schedule_id],
            )?;
        }
        transaction.commit()?;
        Ok(schedules.len())
    }

    pub fn recover_interrupted(&self) -> Result<usize> {
        let connection = self.connect()?;
        Ok(connection.execute(
            "UPDATE jobs SET status='interrupted',finished_at=?,error_message='Worker restarted before the job completed' WHERE status IN ('running','stopping')",
            [now()],
        )?)
    }

    pub fn start_job(&self, id: &str, archives_found: usize) -> Result<()> {
        let connection = self.connect()?;
        connection.execute(
            "UPDATE jobs SET status='running',started_at=COALESCE(started_at,?),archives_found=?,cancel_requested=0 WHERE id=?",
            params![now(), archives_found as i64, id],
        )?;
        Ok(())
    }

    pub fn cancel_job(&self, id: &str) -> Result<bool> {
        let connection = self.connect()?;
        Ok(connection.execute(
            "UPDATE jobs SET status=CASE WHEN status='queued' THEN 'cancelled' ELSE 'stopping' END,cancel_requested=1,finished_at=CASE WHEN status='queued' THEN ? ELSE finished_at END WHERE id=? AND status IN ('queued','running','stopping')",
            params![now(), id],
        )? == 1)
    }

    pub fn cancellation_requested(&self, id: &str) -> Result<bool> {
        let connection = self.connect()?;
        Ok(connection
            .query_row(
                "SELECT cancel_requested FROM jobs WHERE id=?",
                [id],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .unwrap_or_default()
            != 0)
    }

    pub fn finish_job(&self, id: &str, status: JobStatus, error: Option<&str>) -> Result<()> {
        let mut connection = self.connect()?;
        let transaction = connection.transaction()?;
        transaction.execute(
            "UPDATE jobs SET status=?,finished_at=?,cancel_requested=0,error_message=? WHERE id=?",
            params![super::encode(status), now(), error, id],
        )?;
        transaction.execute(
            "INSERT INTO library_snapshots(job_id,library_id,recorded_at,archive_count,optimized_count,failed_count,baseline_bytes,current_bytes,net_bytes,page_count,converted_pages,average_percent_saved) SELECT j.id,j.library_id,?,COUNT(a.id),COALESCE(SUM(CASE WHEN a.status='processed' AND a.converted_pages>0 AND a.bytes_saved>0 THEN 1 ELSE 0 END),0),COALESCE(SUM(CASE WHEN a.status='failed' THEN 1 ELSE 0 END),0),COALESCE(SUM(a.baseline_size),0),COALESCE(SUM(a.current_size),0),COALESCE(SUM(CASE WHEN a.status='processed' THEN a.bytes_saved ELSE 0 END),0),COALESCE(SUM(a.image_count),0),COALESCE(SUM(a.converted_pages),0),COALESCE(AVG(CASE WHEN a.converted_pages>0 AND a.bytes_saved>0 THEN a.percent_saved END),0) FROM jobs j LEFT JOIN archives a ON a.library_id=j.library_id WHERE j.id=? GROUP BY j.id,j.library_id ON CONFLICT(job_id) DO UPDATE SET recorded_at=excluded.recorded_at,archive_count=excluded.archive_count,optimized_count=excluded.optimized_count,failed_count=excluded.failed_count,baseline_bytes=excluded.baseline_bytes,current_bytes=excluded.current_bytes,net_bytes=excluded.net_bytes,page_count=excluded.page_count,converted_pages=excluded.converted_pages,average_percent_saved=excluded.average_percent_saved",
            params![now(),id],
        )?;
        transaction.commit()?;
        Ok(())
    }
}
