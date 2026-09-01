use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use time::OffsetDateTime;

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Scope {
    Library,
    #[default]
    Unprocessed,
    Failed,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Strategy {
    #[default]
    Lossless,
    Smart,
    Lossy,
    Recheck,
    Audit,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(default)]
pub struct Policy {
    pub strategy: Strategy,
    pub scope: Scope,
    pub effort: u8,
    pub threads: usize,
    pub distance: Option<f32>,
    pub smart_mb: f32,
    pub smart_mp: f32,
    pub backup: bool,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            strategy: Strategy::Lossless,
            scope: Scope::Unprocessed,
            effort: 10,
            threads: 4,
            distance: None,
            smart_mb: 10.0,
            smart_mp: 5.0,
            backup: false,
        }
    }
}

impl Policy {
    pub fn normalize(mut self, cpu_limit: usize) -> Self {
        self.effort = self.effort.min(11);
        self.threads = self.threads.clamp(1, cpu_limit.max(1));
        self.distance = self.distance.map(|value| value.clamp(0.0, 15.0));
        self.smart_mb = self.smart_mb.clamp(0.1, 1000.0);
        self.smart_mp = self.smart_mp.clamp(0.1, 1000.0);
        if matches!(self.strategy, Strategy::Audit) {
            self.scope = Scope::Library;
        }
        self
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Queued,
    Running,
    Stopping,
    Succeeded,
    Partial,
    Failed,
    Cancelled,
    Interrupted,
}

impl JobStatus {
    pub fn terminal(self) -> bool {
        !matches!(self, Self::Queued | Self::Running | Self::Stopping)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobSource {
    Manual,
    Schedule,
    Retry,
    Import,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Job {
    pub id: String,
    pub library_id: String,
    pub policy_id: Option<String>,
    pub source: JobSource,
    pub status: JobStatus,
    pub policy: Policy,
    pub only_path: Option<String>,
    pub action: Option<RepairAction>,
    pub queued_at: OffsetDateTime,
    pub cancel_requested: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum RepairAction {
    Compatibility,
    Preserve { entry: String },
    Delete { entry: String },
    Replace { entry: String, source: PathBuf },
    Recover { entry: String },
    RebuildCrc { entries: Vec<String> },
    Salvage { partial: bool },
    Repack,
    Restore { reconvert: bool },
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Optimized,
    Corrected,
    Repaired,
    AlreadyCurrent,
    Retained,
    Audited,
    Failed,
    Deleted,
    Deferred,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ArchiveResult {
    pub path: String,
    pub outcome: Outcome,
    pub source_size: u64,
    pub final_size: u64,
    pub pages: usize,
    pub converted: usize,
    pub retained: usize,
    pub failed: usize,
    pub duration_ms: u64,
    pub fingerprint: Option<String>,
    pub error: Option<String>,
}

impl ArchiveResult {
    pub fn bytes_saved(&self) -> i64 {
        self.source_size as i64 - self.final_size as i64
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PageResult {
    pub entry_path: String,
    pub source_format: Option<String>,
    pub source_size: Option<u64>,
    pub candidate_size: Option<u64>,
    pub final_size: Option<u64>,
    pub outcome: String,
    pub mode: Option<String>,
    pub distance: Option<f32>,
    pub effort: Option<u8>,
    pub reason: Option<String>,
    #[serde(default)]
    pub attempts: Vec<serde_json::Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct IssueDraft {
    pub kind: String,
    pub stage: String,
    pub entry_path: Option<String>,
    pub related_entry_path: Option<String>,
    pub message: String,
    pub detail: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RecordedArchive {
    pub result: ArchiveResult,
    #[serde(default)]
    pub refresh_identity: bool,
    pub mtime_ns: Option<i64>,
    pub input_fingerprint: Option<String>,
    pub final_fingerprint: Option<String>,
    pub mode: Option<String>,
    pub effort: Option<u8>,
    pub pages: Vec<PageResult>,
    pub issues: Vec<IssueDraft>,
    pub formats: Vec<FormatStat>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FormatStat {
    pub format: String,
    pub files: u64,
    pub compressed_bytes: u64,
    pub uncompressed_bytes: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Library {
    pub id: String,
    pub name: String,
    pub root: PathBuf,
    pub enabled: bool,
}

impl Library {
    pub fn resolve(&self, relative: &str) -> anyhow::Result<PathBuf> {
        let relative = Path::new(relative);
        anyhow::ensure!(!relative.is_absolute(), "archive path must be relative");
        anyhow::ensure!(
            !relative
                .components()
                .any(|part| matches!(part, std::path::Component::ParentDir)),
            "archive path escapes the library"
        );
        let root = self.root.canonicalize()?;
        let target = root.join(relative);
        anyhow::ensure!(
            target
                .extension()
                .is_some_and(|value| value.eq_ignore_ascii_case("cbz")),
            "archive path is not a CBZ"
        );
        if target.exists() {
            let resolved = target.canonicalize()?;
            anyhow::ensure!(
                resolved.starts_with(&root),
                "archive path escapes the library"
            );
            anyhow::ensure!(resolved == target, "archive path traverses a symbolic link");
        }
        Ok(target)
    }
}
