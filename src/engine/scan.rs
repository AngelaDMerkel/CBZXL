use crate::{
    db::Database,
    domain::{Job, Library, Scope},
    progress::ProgressHub,
};
use anyhow::{Context, Result};
use std::path::PathBuf;
use walkdir::WalkDir;

#[derive(Debug)]
pub struct DiscoveredArchive {
    pub path: PathBuf,
    pub relative: String,
}

pub enum Discovery {
    Ready(Vec<DiscoveredArchive>),
    Cancelled,
}

pub fn discover(
    database: &Database,
    progress: &ProgressHub,
    job: &Job,
    library: &Library,
) -> Result<Discovery> {
    progress.publish(
        &job.id,
        "discovering",
        serde_json::json!({"archives_found":0}),
    );
    if let Some(relative) = &job.only_path {
        if database.cancellation_requested(&job.id)? {
            return Ok(Discovery::Cancelled);
        }
        let path = library.resolve(relative)?;
        anyhow::ensure!(path.is_file(), "archive does not exist: {relative}");
        return Ok(Discovery::Ready(vec![DiscoveredArchive {
            path,
            relative: relative.clone(),
        }]));
    }
    if matches!(job.policy.scope, Scope::Failed) {
        let archives = database
            .failed_paths(&library.id)?
            .into_iter()
            .filter_map(|relative| {
                library.resolve(&relative).ok().and_then(|path| {
                    path.is_file()
                        .then_some(DiscoveredArchive { path, relative })
                })
            })
            .collect::<Vec<_>>();
        progress.publish(
            &job.id,
            "discovering",
            serde_json::json!({"archives_found":archives.len()}),
        );
        return Ok(if database.cancellation_requested(&job.id)? {
            Discovery::Cancelled
        } else {
            Discovery::Ready(archives)
        });
    }
    let mut archives = Vec::new();
    for entry in WalkDir::new(&library.root)
        .follow_links(false)
        .into_iter()
        .filter_entry(|entry| entry.depth() == 0 || entry.file_name() != ".cbzxl-trash")
    {
        let entry = entry.context("library discovery failed")?;
        if !entry.file_type().is_file()
            || !entry
                .path()
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("cbz"))
        {
            continue;
        }
        archives.push(DiscoveredArchive {
            relative: relative(library, entry.path())?,
            path: entry.into_path(),
        });
        if archives.len() % 2_048 == 0 {
            progress.publish(
                &job.id,
                "discovering",
                serde_json::json!({"archives_found":archives.len()}),
            );
            if database.cancellation_requested(&job.id)? {
                return Ok(Discovery::Cancelled);
            }
        }
    }
    progress.publish(
        &job.id,
        "discovering",
        serde_json::json!({"archives_found":archives.len()}),
    );
    Ok(if database.cancellation_requested(&job.id)? {
        Discovery::Cancelled
    } else {
        Discovery::Ready(archives)
    })
}

pub fn relative(library: &Library, path: &std::path::Path) -> Result<String> {
    Ok(path
        .strip_prefix(&library.root)
        .context("archive is outside the library")?
        .to_string_lossy()
        .replace('\\', "/"))
}
