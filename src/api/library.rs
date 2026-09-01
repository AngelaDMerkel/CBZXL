use super::{ApiError, AppState, query_json};
use crate::{
    domain::{Policy, RepairAction},
    engine::{CommandMediaBackend, MediaBackend},
};
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{OriginalUri, Path, Query, State},
    http::{HeaderValue, header},
    response::Response,
    routing::{get, post},
};
use rusqlite::{OptionalExtension, params};
use serde::Deserialize;
use std::sync::Arc;
use uuid::Uuid;

pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/archives", get(archives))
        .route("/archives/{id}", get(archive))
        .route("/archives/{id}/review", post(review_archive))
        .route("/issues/summary", get(issue_summary))
        .route("/issues", get(issues))
        .route("/issues/{id}/dismiss", post(dismiss_issue))
        .route("/issues/{id}/retry", post(retry_issue))
        .route("/issues/{id}/action", post(issue_action))
        .route("/issues/{id}/diagnostics", get(issue_diagnostics))
        .route("/issues/{id}/preview", get(preview_image))
        .route(
            "/issues/{id}/replacement",
            axum::routing::put(replace_image)
                .layer(axum::extract::DefaultBodyLimit::max(100 * 1024 * 1024)),
        )
}

#[derive(Debug, Deserialize)]
struct ArchiveQuery {
    after_id: Option<i64>,
    search: Option<String>,
    status: Option<String>,
    review: Option<String>,
    sort: Option<String>,
    page: Option<usize>,
    page_size: Option<usize>,
    #[serde(default = "default_limit")]
    limit: usize,
}

async fn archives(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ArchiveQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let limit = query.page_size.unwrap_or(query.limit).clamp(1, 200);
    let search = query.search.unwrap_or_default();
    let pattern = format!(
        "%{}%",
        search
            .replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_")
    );
    let connection = state.database.connect()?;
    let status = query.status.filter(|value| value != "all");
    let review = query.review.filter(|value| value != "all");
    if let Some(page) = query.page {
        let page = page.max(1);
        let order = match query.sort.as_deref() {
            Some("saved") => "bytes_saved DESC",
            Some("path") => "path COLLATE NOCASE",
            Some("duration") => "duration_ms DESC",
            _ => "updated_at DESC",
        };
        let total:i64=connection.query_row("SELECT COUNT(*) FROM archives WHERE (?='' OR path LIKE ? ESCAPE '\\') AND (? IS NULL OR status=?) AND (? IS NULL OR review_status=?)",params![search,pattern,status,status,review,review],|row|row.get(0))?;
        let rows = query_json(
            &connection,
            &format!(
                "SELECT * FROM archives WHERE (?='' OR path LIKE ? ESCAPE '\\') AND (? IS NULL OR status=?) AND (? IS NULL OR review_status=?) ORDER BY {order} LIMIT ? OFFSET ?"
            ),
            params![
                search,
                pattern,
                status,
                status,
                review,
                review,
                limit as i64,
                ((page - 1) * limit) as i64
            ],
        )?;
        return Ok(Json(
            serde_json::json!({"rows":rows,"total":total,"page":page,"page_size":limit,"pages":(total.max(0) as usize).div_ceil(limit).max(1)}),
        ));
    }
    let rows = query_json(
        &connection,
        "SELECT * FROM archives WHERE id>? AND (?='' OR path LIKE ? ESCAPE '\\') AND (? IS NULL OR status=?) AND (? IS NULL OR review_status=?) ORDER BY id LIMIT ?",
        params![
            query.after_id.unwrap_or(0),
            search,
            pattern,
            status,
            status,
            review,
            review,
            limit as i64
        ],
    )?;
    let next = (rows.len() == limit)
        .then(|| {
            rows.last()
                .and_then(|row| row.get("id"))
                .and_then(|id| id.as_i64())
        })
        .flatten();
    Ok(Json(serde_json::json!({"rows":rows,"next":next})))
}

async fn archive(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let connection = state.database.connect()?;
    let archive = query_json(&connection, "SELECT * FROM archives WHERE id=?", [id])?
        .pop()
        .ok_or_else(|| ApiError::not_found(anyhow::anyhow!("archive not found")))?;
    let outcomes = query_json(
        &connection,
        "SELECT * FROM job_archive_outcomes WHERE archive_id=? ORDER BY id DESC LIMIT 100",
        [id],
    )?;
    let issues = query_json(
        &connection,
        "SELECT * FROM issues WHERE archive_id=? ORDER BY id DESC",
        [id],
    )?;
    let formats = query_json(
        &connection,
        "SELECT * FROM archive_formats WHERE archive_id=? ORDER BY format",
        [id],
    )?;
    Ok(Json(
        serde_json::json!({"archive":archive,"outcomes":outcomes,"issues":issues,"formats":formats}),
    ))
}

async fn issue_summary(
    State(state): State<Arc<AppState>>,
    OriginalUri(uri): OriginalUri,
) -> Result<Json<serde_json::Value>, ApiError> {
    let connection = state.database.connect()?;
    let groups = query_json(
        &connection,
        "SELECT kind,stage,COUNT(*) AS issues,COUNT(DISTINCT archive_id) AS archives FROM issues WHERE status IN ('open','queued') GROUP BY kind,stage ORDER BY issues DESC,kind,stage",
        [],
    )?;
    let total = query_json(
        &connection,
        "SELECT COUNT(*) AS issues,COUNT(DISTINCT archive_id) AS archives FROM issues WHERE status IN ('open','queued')",
        [],
    )?.pop().unwrap_or_default();
    if uri.path().starts_with("/api/") && !uri.path().starts_with("/api/v2/") {
        let legacy=groups.iter().map(|row|serde_json::json!({"id":format!("{}:{}",row.get("kind").and_then(|v|v.as_str()).unwrap_or("issue"),row.get("stage").and_then(|v|v.as_str()).unwrap_or("unknown")),"label":format!("{} · {}",row.get("kind").and_then(|v|v.as_str()).unwrap_or("issue").replace('_'," "),row.get("stage").and_then(|v|v.as_str()).unwrap_or("unknown").replace('_'," ")),"count":row.get("issues"),"archive_count":row.get("archives")})).collect::<Vec<_>>();
        return Ok(Json(
            serde_json::json!({"rows":[],"groups":legacy,"total":total.get("archives").cloned().unwrap_or_default()}),
        ));
    }
    Ok(Json(serde_json::json!({"groups":groups,"total":total})))
}

#[derive(Debug, Deserialize)]
struct IssueQuery {
    #[serde(alias = "after_id")]
    before_id: Option<i64>,
    kind: Option<String>,
    group: Option<String>,
    #[serde(default = "default_limit")]
    limit: usize,
}

async fn issues(
    State(state): State<Arc<AppState>>,
    Query(query): Query<IssueQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let limit = query.limit.clamp(1, 200);
    let group_parts = query
        .group
        .as_deref()
        .map(|value| value.split_once(':').unwrap_or((value, "")));
    let kind = query
        .kind
        .or_else(|| group_parts.map(|value| value.0.to_owned()));
    let stage = group_parts.and_then(|value| (!value.1.is_empty()).then(|| value.1.to_owned()));
    let connection = state.database.connect()?;
    let mut rows = query_json(
        &connection,
        "SELECT i.*,a.path,a.library_id,a.status AS archive_status,a.baseline_size AS original_size,a.current_size AS final_size,a.bytes_saved,a.percent_saved,a.image_count,COUNT(*) OVER(PARTITION BY i.archive_id) AS archive_issue_count,CASE WHEN j.source='retry' AND j.status IN ('failed','partial') THEN 1 ELSE 0 END AS retry_failed FROM issues i JOIN archives a ON a.id=i.archive_id LEFT JOIN jobs j ON j.id=i.job_id WHERE i.id<? AND i.status IN ('open','queued') AND (? IS NULL OR i.kind=?) AND (? IS NULL OR i.stage=?) ORDER BY i.id DESC LIMIT ?",
        params![
            query.before_id.unwrap_or(i64::MAX),
            kind,
            kind,
            stage,
            stage,
            limit as i64
        ],
    )?;
    let libraries = state.database.libraries()?;
    for row in &mut rows {
        let kind = row
            .get("kind")
            .and_then(|value| value.as_str())
            .unwrap_or("archive_failed")
            .to_owned();
        let entry = row.get("entry_path").is_some_and(|value| !value.is_null());
        let backup_available = row
            .get("library_id")
            .and_then(|value| value.as_str())
            .and_then(|library_id| libraries.iter().find(|library| library.id == library_id))
            .zip(row.get("path").and_then(|value| value.as_str()))
            .and_then(|(library, path)| library.resolve(path).ok())
            .is_some_and(|path| path.with_extension("cbz.bak").is_file());
        let remedies = issue_remedies(
            &kind,
            entry,
            backup_available,
            row.get("retry_failed")
                .and_then(|value| value.as_i64())
                .unwrap_or(0)
                != 0,
        );
        if let Some(object) = row.as_object_mut() {
            object.insert("backup_available".into(), backup_available.into());
            object.insert("remedies".into(), serde_json::json!(remedies));
            if entry {
                object.insert(
                    "preview_url".into(),
                    format!(
                        "/api/v2/issues/{}/preview",
                        object.get("id").and_then(|v| v.as_i64()).unwrap_or(0)
                    )
                    .into(),
                );
            }
            if object
                .get("related_entry_path")
                .is_some_and(|value| !value.is_null())
            {
                object.insert(
                    "related_preview_url".into(),
                    format!(
                        "/api/v2/issues/{}/preview?related=true",
                        object.get("id").and_then(|v| v.as_i64()).unwrap_or(0)
                    )
                    .into(),
                );
            }
            if matches!(kind.as_str(), "crc_mismatch" | "archive_corrupt") {
                object.insert(
                    "diagnostics_url".into(),
                    format!(
                        "/api/v2/issues/{}/diagnostics",
                        object.get("id").and_then(|v| v.as_i64()).unwrap_or(0)
                    )
                    .into(),
                );
            }
        }
    }
    let next = (rows.len() == limit)
        .then(|| {
            rows.last()
                .and_then(|row| row.get("id"))
                .and_then(|id| id.as_i64())
        })
        .flatten();
    Ok(Json(serde_json::json!({"rows":rows,"next":next})))
}

fn issue_remedies(
    kind: &str,
    entry: bool,
    backup_available: bool,
    retry_failed: bool,
) -> Vec<&'static str> {
    let mut values = match kind {
        "crc_mismatch" => vec!["rebuild", "preserve", "replace", "delete"],
        "filename_collision" => vec!["keep_primary", "keep_related"],
        "archive_corrupt" => vec!["salvage", "salvage_partial", "compatibility"],
        "archive_grew" if backup_available => {
            vec!["lossless", "smart_lossy", "restore_original", "repack"]
        }
        "archive_grew" => vec!["repack"],
        _ if entry => vec!["recover", "preserve", "replace", "delete"],
        _ => vec!["compatibility"],
    };
    if retry_failed {
        values.push("delete_archive");
    }
    values.push("dismiss");
    values
}

async fn dismiss_issue(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let connection = state.database.connect()?;
    let issue: Option<i64> = connection
        .query_row(
            "SELECT archive_id FROM issues WHERE id=? AND status IN ('open','queued')",
            [id],
            |row| row.get(0),
        )
        .optional()?;
    let archive_id =
        issue.ok_or_else(|| ApiError::not_found(anyhow::anyhow!("issue not found")))?;
    let now = crate::db::now();
    connection.execute(
        "UPDATE issues SET status='dismissed',remedy='dismiss',updated_at=? WHERE id=?",
        params![now, id],
    )?;
    let remaining: i64 = connection.query_row(
        "SELECT COUNT(*) FROM issues WHERE archive_id=? AND status IN ('open','queued')",
        [archive_id],
        |row| row.get(0),
    )?;
    if remaining == 0 {
        connection.execute("UPDATE archives SET review_status='reviewed',review_note='Dismissed until full rescan',dismissed_until_rescan=1,reviewed_at=?,updated_at=? WHERE id=?", params![now,now,archive_id])?;
    }
    Ok(Json(serde_json::json!({"status":"dismissed"})))
}

async fn retry_issue(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let mut connection = state.database.connect()?;
    let issue: Option<(i64,String,String,String)> = connection
        .query_row("SELECT i.archive_id,a.library_id,a.path,i.kind FROM issues i JOIN archives a ON a.id=i.archive_id WHERE i.id=?", [id], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?)))
        .optional()?;
    let (archive_id, library_id, path, kind) =
        issue.ok_or_else(|| ApiError::not_found(anyhow::anyhow!("issue not found")))?;
    let (policy_id, policy_json): (String, String) = connection.query_row(
        "SELECT id,policy_json FROM policies ORDER BY is_default DESC,created_at LIMIT 1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let policy: Policy = serde_json::from_str(&policy_json)?;
    let action = if kind == "archive_grew" {
        let backup_available = state
            .database
            .libraries()?
            .into_iter()
            .find(|library| library.id == library_id)
            .and_then(|library| library.resolve(&path).ok())
            .is_some_and(|source| source.with_extension("cbz.bak").is_file());
        Some(if backup_available {
            RepairAction::Restore { reconvert: true }
        } else {
            RepairAction::Repack
        })
    } else {
        Some(RepairAction::Compatibility)
    };
    let transaction = connection.transaction()?;
    let job_id = enqueue_repair_in(
        &transaction,
        &library_id,
        Some(&policy_id),
        &policy,
        &path,
        action.as_ref(),
    )?;
    transaction.execute(
        "UPDATE issues SET status='queued',remedy='retry',job_id=?,updated_at=? WHERE id=?",
        params![job_id, crate::db::now(), id],
    )?;
    transaction.execute(
        "UPDATE archives SET review_status='queued',updated_at=? WHERE id=?",
        params![crate::db::now(), archive_id],
    )?;
    transaction.commit()?;
    Ok(Json(serde_json::json!({"id":job_id,"status":"queued"})))
}

const fn default_limit() -> usize {
    100
}

#[derive(Deserialize)]
struct ActionInput {
    action: String,
}

type ActionIssue = (i64, String, String, String, Option<String>, Option<String>);
type DiagnosticIssue = (
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
);

async fn issue_action(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
    Json(input): Json<ActionInput>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let mut connection = state.database.connect()?;
    let issue: Option<ActionIssue> = connection.query_row(
        "SELECT i.archive_id,a.library_id,a.path,i.kind,i.entry_path,i.related_entry_path FROM issues i JOIN archives a ON a.id=i.archive_id WHERE i.id=? AND i.status IN ('open','queued')",
        [id],
        |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?)),
    ).optional()?;
    let (archive_id, library_id, path, kind, entry, related_entry) =
        issue.ok_or_else(|| ApiError::not_found(anyhow::anyhow!("issue not found")))?;
    if input.action == "delete_archive" {
        let retry_failed = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM issues i JOIN jobs j ON j.id=i.job_id WHERE i.id=? AND j.source='retry' AND j.status IN ('failed','partial'))",
            [id],
            |row| row.get::<_, i64>(0),
        )? != 0;
        if !retry_failed {
            return Err(ApiError::conflict(anyhow::anyhow!(
                "archive deletion is available only after a failed retry"
            )));
        }
        let library = state
            .database
            .libraries()?
            .into_iter()
            .find(|value| value.id == library_id)
            .ok_or_else(|| ApiError::conflict(anyhow::anyhow!("library is unavailable")))?;
        let source = library.resolve(&path).map_err(ApiError::bad_request)?;
        let trash = library.root.join(".cbzxl-trash");
        std::fs::create_dir_all(&trash)?;
        let target = trash.join(format!(
            "{}-{}",
            Uuid::new_v4(),
            source.file_name().unwrap_or_default().to_string_lossy()
        ));
        std::fs::rename(&source, &target)?;
        if let Err(error) = connection.execute("DELETE FROM archives WHERE id=?", [archive_id]) {
            let _ = std::fs::rename(&target, &source);
            return Err(error.into());
        }
        return Ok(Json(serde_json::json!({"status":"trashed","path":target})));
    }
    let require_entry = || {
        entry.clone().ok_or_else(|| {
            ApiError::conflict(anyhow::anyhow!("this repair requires an image entry"))
        })
    };
    let action = match input.action.as_str() {
        "compatibility" => RepairAction::Compatibility,
        "preserve" => RepairAction::Preserve {
            entry: require_entry()?,
        },
        "delete" => RepairAction::Delete {
            entry: require_entry()?,
        },
        "recover" => RepairAction::Recover {
            entry: require_entry()?,
        },
        "keep_primary" if kind == "filename_collision" => RepairAction::Delete {
            entry: related_entry.ok_or_else(|| {
                ApiError::conflict(anyhow::anyhow!("related collision entry is missing"))
            })?,
        },
        "keep_related" if kind == "filename_collision" => RepairAction::Delete {
            entry: require_entry()?,
        },
        "rebuild" if kind == "crc_mismatch" => {
            let mut statement=connection.prepare("SELECT entry_path FROM issues WHERE archive_id=? AND kind='crc_mismatch' AND status IN ('open','queued') AND entry_path IS NOT NULL ORDER BY entry_path")?;
            let entries = statement
                .query_map([archive_id], |row| row.get(0))?
                .collect::<rusqlite::Result<_>>()?;
            RepairAction::RebuildCrc { entries }
        }
        "salvage" => RepairAction::Salvage { partial: false },
        "salvage_partial" => RepairAction::Salvage { partial: true },
        "repack" => RepairAction::Repack,
        "restore_original" => RepairAction::Restore { reconvert: false },
        "lossless" | "smart_lossy" => RepairAction::Restore { reconvert: true },
        _ => {
            return Err(ApiError::bad_request(anyhow::anyhow!(
                "unsupported repair action"
            )));
        }
    };
    let (policy_id, policy_json): (String, String) = connection.query_row(
        "SELECT id,policy_json FROM policies ORDER BY is_default DESC,created_at LIMIT 1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let mut policy: Policy = serde_json::from_str(&policy_json)?;
    if input.action == "smart_lossy" {
        policy.strategy = crate::domain::Strategy::Smart;
        policy.distance = Some(0.5);
    }
    if input.action == "lossless" {
        policy.strategy = crate::domain::Strategy::Lossless;
        policy.distance = None;
    }
    let transaction = connection.transaction()?;
    let job_id = enqueue_repair_in(
        &transaction,
        &library_id,
        Some(&policy_id),
        &policy,
        &path,
        Some(&action),
    )?;
    transaction.execute(
        "UPDATE issues SET status='queued',remedy=?,job_id=?,updated_at=? WHERE id=?",
        params![input.action, job_id, crate::db::now(), id],
    )?;
    transaction.execute(
        "UPDATE archives SET review_status='queued',updated_at=? WHERE id=?",
        params![crate::db::now(), archive_id],
    )?;
    transaction.commit()?;
    Ok(Json(serde_json::json!({"id":job_id,"status":"queued"})))
}

async fn replace_image(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
    body: Bytes,
) -> Result<Json<serde_json::Value>, ApiError> {
    if body.is_empty() {
        return Err(ApiError::bad_request(anyhow::anyhow!(
            "replacement image is empty"
        )));
    }
    let mut connection = state.database.connect()?;
    let issue:Option<(i64,String,String,String)>=connection.query_row("SELECT i.archive_id,a.library_id,a.path,i.entry_path FROM issues i JOIN archives a ON a.id=i.archive_id WHERE i.id=? AND i.entry_path IS NOT NULL",[id],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?))).optional()?;
    let (archive_id, library_id, path, entry) =
        issue.ok_or_else(|| ApiError::not_found(anyhow::anyhow!("image issue not found")))?;
    let root = state
        .database
        .path()
        .parent()
        .unwrap_or(std::path::Path::new("/config"))
        .join("replacements");
    std::fs::create_dir_all(&root)?;
    let staged = root.join(format!(
        "{}-{}",
        Uuid::new_v4(),
        std::path::Path::new(&entry)
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
    ));
    tokio::fs::write(&staged, &body).await?;
    let backend =
        CommandMediaBackend::new(state.config.encoder_timeout, state.config.auxiliary_timeout);
    let signature = body.as_ref();
    let validation =
        if signature.starts_with(&[0xff, 0x0a]) || signature.starts_with(b"\0\0\0\x0cJXL ") {
            backend.decode_jxl(&staged).await
        } else {
            backend.info(&staged).await.map(|_| ())
        };
    if let Err(error) = validation {
        let _ = tokio::fs::remove_file(&staged).await;
        return Err(ApiError::bad_request(
            error.context("replacement image is unreadable"),
        ));
    }
    let (policy_id, policy_json): (String, String) = connection.query_row(
        "SELECT id,policy_json FROM policies ORDER BY is_default DESC,created_at LIMIT 1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let policy: Policy = serde_json::from_str(&policy_json)?;
    let transaction = connection.transaction()?;
    let action = RepairAction::Replace {
        entry,
        source: staged.clone(),
    };
    let job_id = match enqueue_repair_in(
        &transaction,
        &library_id,
        Some(&policy_id),
        &policy,
        &path,
        Some(&action),
    ) {
        Ok(job_id) => job_id,
        Err(error) => {
            drop(transaction);
            let _ = std::fs::remove_file(&staged);
            return Err(error.into());
        }
    };
    let persisted = (|| -> anyhow::Result<()> {
        transaction.execute(
            "UPDATE issues SET status='queued',remedy='replace',job_id=?,updated_at=? WHERE id=?",
            params![job_id, crate::db::now(), id],
        )?;
        transaction.execute(
            "UPDATE archives SET review_status='queued',updated_at=? WHERE id=?",
            params![crate::db::now(), archive_id],
        )?;
        transaction.commit()?;
        Ok(())
    })();
    if let Err(error) = persisted {
        let _ = std::fs::remove_file(&staged);
        return Err(error.into());
    }
    Ok(Json(serde_json::json!({"id":job_id,"status":"queued"})))
}

fn enqueue_repair_in(
    transaction: &rusqlite::Transaction<'_>,
    library_id: &str,
    policy_id: Option<&str>,
    policy: &Policy,
    path: &str,
    action: Option<&RepairAction>,
) -> anyhow::Result<String> {
    let id = Uuid::new_v4().to_string();
    transaction.execute(
        "INSERT INTO jobs(id,source,library_id,policy_id,status,policy_json,only_path,action_json,queued_at) VALUES (?,'retry',?,?,'queued',?,?,?,?)",
        params![id,library_id,policy_id,serde_json::to_string(policy)?,path,action.map(serde_json::to_string).transpose()?,crate::db::now()],
    )?;
    Ok(id)
}

#[derive(Deserialize)]
struct ReviewInput {
    action: String,
    note: Option<String>,
}

async fn review_archive(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
    Json(input): Json<ReviewInput>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let mut connection = state.database.connect()?;
    let archive: Option<(String, String)> = connection
        .query_row(
            "SELECT library_id,path FROM archives WHERE id=?",
            [id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let (library_id, path) =
        archive.ok_or_else(|| ApiError::not_found(anyhow::anyhow!("archive not found")))?;
    let now = crate::db::now();
    let mut job_id = None;
    let transaction = connection.transaction()?;
    match input.action.as_str() {
        "reviewed" => {
            transaction.execute(
                "UPDATE archives SET review_status='reviewed',review_note=?,reviewed_at=?,updated_at=? WHERE id=?",
                params![input.note,now,now,id],
            )?;
        }
        "reopen" => {
            transaction.execute(
                "UPDATE archives SET review_status='open',review_note=?,reviewed_at=NULL,updated_at=? WHERE id=?",
                params![input.note,now,id],
            )?;
        }
        "recheck" => {
            let (policy_id, policy_json): (String, String) = transaction.query_row(
                "SELECT id,policy_json FROM policies ORDER BY is_default DESC,created_at,id LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            let mut policy: Policy = serde_json::from_str(&policy_json)?;
            policy.strategy = crate::domain::Strategy::Recheck;
            policy.scope = crate::domain::Scope::Library;
            let queued = enqueue_repair_in(
                &transaction,
                &library_id,
                Some(&policy_id),
                &policy,
                &path,
                None,
            )?;
            transaction.execute(
                "UPDATE archives SET review_status='queued',review_note=?,updated_at=? WHERE id=?",
                params![input.note, now, id],
            )?;
            transaction.execute("INSERT INTO job_events(job_id,created_at,level,event_type,message,archive_path) VALUES (?,?,'info','archive_recheck',?,?)",rusqlite::params![queued,now,format!("Recheck requested for {path}"),path])?;
            job_id = Some(queued);
        }
        _ => {
            return Err(ApiError::bad_request(anyhow::anyhow!(
                "unsupported review action"
            )));
        }
    }
    transaction.execute(
        "INSERT INTO review_actions(archive_id,job_id,action,note,created_at) VALUES (?,?,?,?,?)",
        params![id, job_id, input.action, input.note, now],
    )?;
    transaction.commit()?;
    Ok(Json(
        serde_json::json!({"status":"success","job_id":job_id}),
    ))
}

#[derive(Deserialize, Default)]
struct PreviewQuery {
    #[serde(default)]
    related: bool,
    entry: Option<String>,
}

async fn issue_diagnostics(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let connection = state.database.connect()?;
    let issue: Option<DiagnosticIssue> = connection
        .query_row(
            "SELECT i.kind,a.library_id,a.path,i.entry_path,i.related_entry_path,i.detail FROM issues i JOIN archives a ON a.id=i.archive_id WHERE i.id=?",
            [id],
            |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?)),
        )
        .optional()?;
    let (kind, library_id, path, entry, related, detail) =
        issue.ok_or_else(|| ApiError::not_found(anyhow::anyhow!("issue not found")))?;
    let library = state
        .database
        .libraries()?
        .into_iter()
        .find(|value| value.id == library_id)
        .ok_or_else(|| ApiError::conflict(anyhow::anyhow!("library is unavailable")))?;
    let archive_path = library.resolve(&path).map_err(ApiError::bad_request)?;
    if kind == "crc_mismatch" {
        let entry =
            entry.ok_or_else(|| ApiError::conflict(anyhow::anyhow!("CRC entry is missing")))?;
        let read_path = archive_path.clone();
        let read_entry = entry.clone();
        let (stored_crc, payload) = tokio::task::spawn_blocking(move || {
            let mut archive = zip::ZipArchive::new(std::fs::File::open(&read_path)?)?;
            let stored_crc = archive.by_name(&read_entry)?.crc32();
            drop(archive);
            let payload =
                crate::engine::read_archive_entry(&read_path, &read_entry, 128 * 1024 * 1024)?;
            Ok::<_, anyhow::Error>((stored_crc, payload))
        })
        .await?
        .map_err(ApiError::bad_request)?;
        let calculated_crc = crc32fast::hash(&payload);
        let source = tempfile::Builder::new()
            .suffix(
                std::path::Path::new(&entry)
                    .extension()
                    .and_then(|value| value.to_str())
                    .map(|value| format!(".{value}"))
                    .as_deref()
                    .unwrap_or(".image"),
            )
            .tempfile()?;
        std::fs::write(source.path(), &payload)?;
        let backend =
            CommandMediaBackend::new(state.config.encoder_timeout, state.config.auxiliary_timeout);
        let jxl = payload.starts_with(&[0xff, 0x0a]) || payload.starts_with(b"\0\0\0\x0cJXL ");
        let repairable = if jxl {
            backend.decode_jxl(source.path()).await.is_ok()
        } else {
            backend.info(source.path()).await.is_ok()
        };
        return Ok(Json(serde_json::json!({
            "repairable":repairable,"entry_path":entry,"stored_crc":stored_crc,
            "calculated_crc":calculated_crc,"payload_bytes":payload.len(),"detail":detail,
            "kind":"crc","expected_crc32":format!("{stored_crc:08X}"),"calculated_crc32":format!("{calculated_crc:08X}"),
            "image_readable":repairable,"verified_count":if repairable{1}else{0},"checked_entries":1,
            "entries":[{"entry_path":entry,"repairable":repairable,"stored_crc":stored_crc,"calculated_crc":calculated_crc}]
        })));
    }
    if kind == "archive_corrupt" {
        let limit = state.config.archive_limit;
        let mut value = tokio::task::spawn_blocking(move || {
            crate::engine::inspect_salvage(&archive_path, limit)
        })
        .await?;
        if let Some(object) = value.as_object_mut() {
            object.insert("detail".into(), detail.into());
        }
        return Ok(Json(value));
    }
    Ok(Json(
        serde_json::json!({"detail":detail,"entry_path":entry,"related_entry_path":related}),
    ))
}

async fn preview_image(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
    Query(query): Query<PreviewQuery>,
) -> Result<Response, ApiError> {
    let connection = state.database.connect()?;
    let issue:Option<(String,String,Option<String>,Option<String>)>=connection.query_row("SELECT a.library_id,a.path,i.entry_path,i.related_entry_path FROM issues i JOIN archives a ON a.id=i.archive_id WHERE i.id=?",[id],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?))).optional()?;
    let (library_id, path, primary, related) =
        issue.ok_or_else(|| ApiError::not_found(anyhow::anyhow!("image issue not found")))?;
    let use_related = query.related || query.entry.as_deref() == Some("related");
    let entry = if use_related { related } else { primary }
        .ok_or_else(|| ApiError::not_found(anyhow::anyhow!("image entry is unavailable")))?;
    let library = state
        .database
        .libraries()?
        .into_iter()
        .find(|value| value.id == library_id)
        .ok_or_else(|| ApiError::conflict(anyhow::anyhow!("library is unavailable")))?;
    let archive = library.resolve(&path).map_err(ApiError::bad_request)?;
    let read_entry = entry.clone();
    let payload = tokio::task::spawn_blocking(move || {
        crate::engine::read_archive_entry(&archive, &read_entry, 64 * 1024 * 1024)
    })
    .await?
    .map_err(ApiError::bad_request)?;
    let mime = browser_mime(&payload);
    let (bytes, mime) = if let Some(mime) = mime {
        (payload, mime)
    } else {
        let source = tempfile::Builder::new()
            .suffix(
                std::path::Path::new(&entry)
                    .extension()
                    .and_then(|value| value.to_str())
                    .map(|value| format!(".{value}"))
                    .as_deref()
                    .unwrap_or(".image"),
            )
            .tempfile()?;
        std::fs::write(source.path(), payload)?;
        let output = tempfile::Builder::new().suffix(".png").tempfile()?;
        let signature = std::fs::read(source.path())?;
        let jxl = signature.starts_with(&[0xff, 0x0a]) || signature.starts_with(b"\0\0\0\x0cJXL ");
        let mut command = tokio::process::Command::new(if jxl { "djxl" } else { "convert" });
        command
            .arg(source.path())
            .arg(output.path())
            .kill_on_drop(true);
        let result = tokio::time::timeout(std::time::Duration::from_secs(45), command.output())
            .await
            .map_err(|_| ApiError::bad_request(anyhow::anyhow!("preview rendering timed out")))??;
        if !result.status.success() {
            return Err(ApiError::bad_request(anyhow::anyhow!(
                "image could not be rendered"
            )));
        }
        let bytes = std::fs::read(output.path())?;
        if bytes.len() > 64 * 1024 * 1024 {
            return Err(ApiError::bad_request(anyhow::anyhow!(
                "rendered preview is too large"
            )));
        }
        (bytes, "image/png")
    };
    let mut response = Response::new(Body::from(bytes));
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(mime));
    Ok(response)
}

fn browser_mime(payload: &[u8]) -> Option<&'static str> {
    if payload.starts_with(&[0xff, 0xd8, 0xff]) {
        Some("image/jpeg")
    } else if payload.starts_with(b"\x89PNG\r\n\x1a\n") {
        Some("image/png")
    } else if payload.starts_with(b"GIF87a") || payload.starts_with(b"GIF89a") {
        Some("image/gif")
    } else if payload.len() >= 12 && payload.starts_with(b"RIFF") && &payload[8..12] == b"WEBP" {
        Some("image/webp")
    } else {
        None
    }
}
