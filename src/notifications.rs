use crate::{config::Config, db::Database};
use anyhow::Result;
use rusqlite::OptionalExtension;

type JobSummary = (String, String, i64, i64, i64, i64, i64, Option<String>);

pub async fn publish(config: &Config, database: &Database, job_id: &str) -> Result<bool> {
    let Some(url) = &config.ntfy_url else {
        return Ok(false);
    };
    let connection = database.connect()?;
    let job: Option<JobSummary> = connection
        .query_row(
            "SELECT j.status,l.name,j.processed,j.skipped,j.failed,j.deleted,j.net_bytes_saved,j.error_message FROM jobs j JOIN libraries l ON l.id=j.library_id WHERE j.id=?",
            [job_id],
            |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?,row.get(6)?,row.get(7)?)),
        )
        .optional()?;
    let Some((status, library, processed, skipped, failed, deleted, net, error)) = job else {
        return Ok(false);
    };
    if matches!(status.as_str(), "queued" | "running" | "stopping") {
        return Ok(false);
    }
    let (title, priority, tag) = match status.as_str() {
        "succeeded" => ("CBZXL job completed", "default", "white_check_mark"),
        "partial" => ("CBZXL job needs attention", "high", "warning"),
        "cancelled" => ("CBZXL job cancelled", "default", "stop_sign"),
        _ => ("CBZXL job failed", "max", "x"),
    };
    let mut message = format!(
        "{library}: job {status}.\nProcessed {processed}, skipped {skipped}, failed {failed}, deleted {deleted}.\nNet space {}: {}.",
        if net >= 0 { "saved" } else { "increased" },
        format_bytes(net.unsigned_abs())
    );
    if let Some(error) = error.filter(|_| status != "succeeded") {
        message.push_str(&format!(
            "\nFailure: {}",
            error.chars().take(500).collect::<String>()
        ));
    }
    let client = reqwest::Client::builder()
        .timeout(config.ntfy_timeout)
        .build()?;
    let mut request = client
        .post(url)
        .header("Title", title)
        .header("Priority", priority)
        .header("Tags", tag)
        .body(message);
    if let Some(token) = &config.ntfy_token {
        request = request.bearer_auth(token);
    }
    request.send().await?.error_for_status()?;
    database.add_event(
        job_id,
        "success",
        "notification_sent",
        "ntfy notification sent",
        None,
        None,
    )?;
    Ok(true)
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit < 2 {
        format!("{value:.0} {}", UNITS[unit])
    } else {
        format!("{value:.2} {}", UNITS[unit])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sizes_are_readable() {
        assert_eq!(format_bytes(1024), "1 KB");
        assert_eq!(format_bytes(1024 * 1024), "1.00 MB");
    }
}
