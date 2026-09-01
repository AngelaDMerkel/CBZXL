use crate::db::now;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::sync::broadcast;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ProgressEvent {
    pub sequence: u64,
    pub job_id: String,
    pub event: String,
    pub at: String,
    #[serde(flatten)]
    pub data: Value,
}

#[derive(Clone)]
pub struct ProgressHub {
    sequence: Arc<AtomicU64>,
    sender: broadcast::Sender<ProgressEvent>,
    latest: Arc<Mutex<HashMap<String, ProgressEvent>>>,
    runtime: Option<PathBuf>,
}

impl Default for ProgressHub {
    fn default() -> Self {
        let (sender, _) = broadcast::channel(512);
        Self {
            sequence: Arc::new(AtomicU64::new(0)),
            sender,
            latest: Arc::new(Mutex::new(HashMap::new())),
            runtime: None,
        }
    }
}

impl ProgressHub {
    pub fn at(runtime: PathBuf) -> Self {
        Self {
            runtime: Some(runtime),
            ..Self::default()
        }
    }

    pub fn publish(&self, job_id: &str, event: &str, data: Value) {
        let mut event = ProgressEvent {
            sequence: self.sequence.fetch_add(1, Ordering::Relaxed) + 1,
            job_id: job_id.into(),
            event: event.into(),
            at: now(),
            data,
        };
        if let Ok(mut latest) = self.latest.lock() {
            if let (Some(previous), Value::Object(current)) = (latest.get(job_id), &mut event.data)
                && let Value::Object(previous) = &previous.data
            {
                for key in [
                    "archives_found",
                    "completed",
                    "total",
                    "progress_percent",
                    "rate_per_minute",
                    "eta_seconds",
                ] {
                    if let Some(value) = previous.get(key) {
                        current.entry(key).or_insert_with(|| value.clone());
                    }
                }
            }
            latest.insert(job_id.into(), event.clone());
        }
        if let Some(root) = &self.runtime
            && std::fs::create_dir_all(root).is_ok()
        {
            let target = root.join(format!("{job_id}.json"));
            let temporary = root.join(format!(".{job_id}.json.tmp"));
            if serde_json::to_vec(&event)
                .is_ok_and(|bytes| std::fs::write(&temporary, bytes).is_ok())
            {
                let _ = std::fs::rename(temporary, target);
            }
        }
        let _ = self.sender.send(event);
    }

    pub fn subscribe(&self) -> broadcast::Receiver<ProgressEvent> {
        self.sender.subscribe()
    }

    pub fn latest(&self, job_id: &str) -> Option<ProgressEvent> {
        if let Some(value) = self.latest.lock().ok()?.get(job_id).cloned() {
            return Some(value);
        }
        let path = self.runtime.as_ref()?.join(format!("{job_id}.json"));
        serde_json::from_slice(&std::fs::read(path).ok()?).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn latest_progress_retains_rate_when_the_archive_changes() {
        let hub = ProgressHub::default();
        hub.publish(
            "job",
            "job_progress",
            json!({"completed":12,"rate_per_minute":18.0,"eta_seconds":60}),
        );
        hub.publish("job", "archive_started", json!({"archive":"next.cbz"}));
        let latest = hub.latest("job").unwrap();
        assert_eq!(latest.data["archive"], "next.cbz");
        assert_eq!(latest.data["completed"], 12);
        assert_eq!(latest.data["rate_per_minute"], 18.0);
        assert!(latest.data.get("entry").is_none());
    }

    #[test]
    fn archive_transition_drops_stale_page_fields() {
        let hub = ProgressHub::default();
        hub.publish(
            "job",
            "page_completed",
            json!({"archive":"first.cbz","page":4,"total_pages":10,"entry":"004.jpg"}),
        );
        hub.publish("job", "archive_started", json!({"archive":"second.cbz"}));
        let latest = hub.latest("job").unwrap();
        assert_eq!(latest.data["archive"], "second.cbz");
        assert!(latest.data.get("page").is_none());
        assert!(latest.data.get("entry").is_none());
        assert!(latest.data.get("total_pages").is_none());
    }

    #[test]
    fn runtime_progress_is_shared_between_process_hubs() {
        let root = tempfile::tempdir().unwrap();
        let writer = ProgressHub::at(root.path().to_owned());
        writer.publish("job", "job_progress", json!({"completed":42}));
        let reader = ProgressHub::at(root.path().to_owned());
        assert_eq!(reader.latest("job").unwrap().data["completed"], 42);
    }
}
