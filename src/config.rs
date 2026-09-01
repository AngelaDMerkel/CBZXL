use anyhow::{Context, Result};
use std::{env, path::PathBuf, time::Duration};

#[derive(Clone, Debug)]
pub struct Config {
    pub database: PathBuf,
    pub library: PathBuf,
    pub runtime: PathBuf,
    pub scratch: PathBuf,
    pub frontend: PathBuf,
    pub listen: String,
    pub api_token: Option<String>,
    pub timezone: String,
    pub cpu_limit: usize,
    pub archive_limit: u64,
    pub stable_for: Duration,
    pub poll_every: Duration,
    pub encoder_timeout: Duration,
    pub auxiliary_timeout: Duration,
    pub fingerprint_audit_days: u64,
    pub ntfy_url: Option<String>,
    pub ntfy_token: Option<String>,
    pub ntfy_timeout: Duration,
    pub page_outcome_retention_days: u64,
    pub durable_checkpoint: Duration,
    pub durable_checkpoint_archives: usize,
    pub database_maintenance: Duration,
    pub statistics_cache: Duration,
    pub worker_stall: Duration,
    pub auto_retry_attempts: u8,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let config_dir = path("CONFIG_DIR", "/config");
        let detected_cpus = available_cpus();
        Ok(Self {
            database: env::var_os("DB_PATH")
                .map(PathBuf::from)
                .unwrap_or_else(|| config_dir.join("cbzxl-v5.sqlite3")),
            library: path("LIBRARY_PATH", "/data"),
            runtime: path("RUNTIME_DIR", "/runtime"),
            scratch: env::var_os("WORKSPACE_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| path("SCRATCH_DIR", "/dev/shm/cbzxl")),
            frontend: path("FRONTEND_DIR", "/app/frontend/dist"),
            listen: env::var("LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:5000".into()),
            api_token: env::var("CBZXL_API_TOKEN").ok().filter(|v| !v.is_empty()),
            timezone: env::var("TZ").unwrap_or_else(|_| "UTC".into()),
            cpu_limit: number("CPU_LIMIT", detected_cpus)?.clamp(1, detected_cpus),
            archive_limit: number::<u64>("MAX_ARCHIVE_UNCOMPRESSED_MB", 8192)?
                .saturating_mul(1024 * 1024),
            stable_for: Duration::from_secs(number("ARCHIVE_STABLE_SECONDS", 90)?),
            poll_every: Duration::from_millis(
                number(
                    "WORKER_POLL_MS",
                    number::<u64>("WORKER_POLL_SECONDS", 1)?.saturating_mul(1_000),
                )?
                .max(100),
            ),
            encoder_timeout: Duration::from_secs(number("ENCODER_MAX_SECONDS", 0)?),
            auxiliary_timeout: Duration::from_secs(number("AUXILIARY_TIMEOUT_SECONDS", 300)?),
            fingerprint_audit_days: number("FINGERPRINT_AUDIT_DAYS", 30)?.max(1),
            ntfy_url: env::var("NTFY_URL").ok().filter(|value| !value.is_empty()),
            ntfy_token: env::var("NTFY_TOKEN")
                .ok()
                .filter(|value| !value.is_empty()),
            ntfy_timeout: Duration::from_secs(number("NTFY_TIMEOUT_SECONDS", 10)?.clamp(1, 30)),
            page_outcome_retention_days: number(
                "PAGE_OUTCOME_RETENTION_DAYS",
                number("PAGE_DECISION_RETENTION_DAYS", 180)?,
            )?
            .max(1),
            durable_checkpoint: Duration::from_secs(
                number("DURABLE_CHECKPOINT_SECONDS", 300)?.max(300),
            ),
            durable_checkpoint_archives: number("DURABLE_CHECKPOINT_ARCHIVES", 10_000)?.max(1),
            database_maintenance: Duration::from_secs(
                number("DATABASE_MAINTENANCE_SECONDS", 604_800)?.max(86_400),
            ),
            statistics_cache: Duration::from_secs(number("STATISTICS_CACHE_SECONDS", 30)?),
            worker_stall: Duration::from_secs(number("WORKER_STALL_SECONDS", 900)?),
            auto_retry_attempts: number::<u8>("AUTO_RETRY_ATTEMPTS", 1)?.min(1),
        })
    }
}

fn path(name: &str, default: &str) -> PathBuf {
    env::var_os(name)
        .map(PathBuf::from)
        .unwrap_or_else(|| default.into())
}

fn number<T>(name: &str, default: T) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match env::var(name) {
        Ok(value) => value.parse().with_context(|| format!("invalid {name}")),
        Err(_) => Ok(default),
    }
}

fn available_cpus() -> usize {
    let mut limits = vec![std::thread::available_parallelism().map_or(1, usize::from)];
    if let Ok(value) = std::fs::read_to_string("/sys/fs/cgroup/cpu.max") {
        let mut values = value.split_whitespace();
        if let (Some(quota), Some(period)) = (values.next(), values.next())
            && quota != "max"
            && let (Ok(quota), Ok(period)) = (quota.parse::<u64>(), period.parse::<u64>())
            && let Some(value) = quota.checked_div(period)
        {
            limits.push(value.max(1) as usize);
        }
    }
    if let (Ok(quota), Ok(period)) = (
        std::fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_quota_us"),
        std::fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_period_us"),
    ) && let (Ok(quota), Ok(period)) =
        (quota.trim().parse::<i64>(), period.trim().parse::<i64>())
        && quota > 0
        && let Some(value) = quota.checked_div(period)
    {
        limits.push(value.max(1) as usize);
    }
    for path in [
        "/sys/fs/cgroup/cpuset.cpus.effective",
        "/sys/fs/cgroup/cpuset/cpuset.cpus",
    ] {
        if let Ok(value) = std::fs::read_to_string(path)
            && let Some(count) = cpuset_count(&value)
        {
            limits.push(count);
            break;
        }
    }
    limits.into_iter().min().unwrap_or(1).max(1)
}

fn cpuset_count(value: &str) -> Option<usize> {
    let mut count = 0_usize;
    for part in value.trim().split(',') {
        if let Some((start, end)) = part.split_once('-') {
            let (start, end): (usize, usize) = (start.parse().ok()?, end.parse().ok()?);
            if end < start {
                return None;
            }
            count += end - start + 1;
        } else if !part.is_empty() {
            part.parse::<usize>().ok()?;
            count += 1;
        }
    }
    (count > 0).then_some(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parses_cpuset_ranges() {
        assert_eq!(cpuset_count("0-3,6,8-9"), Some(7));
        assert_eq!(cpuset_count(""), None);
        assert_eq!(cpuset_count("3-1"), None);
    }
}
