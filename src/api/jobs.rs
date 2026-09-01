use super::{ApiError, AppState, decode_json_field, query_json};
use crate::domain::{JobSource, Policy, Scope, Strategy};
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    response::sse::{Event, KeepAlive, Sse},
    routing::{get, post},
};
use futures::{Stream, StreamExt};
use rusqlite::{OptionalExtension, params};
use serde::Deserialize;
use std::{convert::Infallible, sync::Arc, time::Duration};
use tokio_stream::wrappers::BroadcastStream;

pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/jobs", get(list).post(create))
        .route("/jobs/{id}", get(detail))
        .route("/jobs/{id}/live", get(live))
        .route("/jobs/{id}/history", get(history))
        .route("/jobs/{id}/page-decisions", get(page_decisions))
        .route("/jobs/{id}/cancel", post(cancel))
        .route("/jobs/{id}/retry", post(retry))
        .route("/jobs/{id}/events", get(events))
}

#[derive(Debug, Deserialize)]
struct ListQuery {
    before_id: Option<String>,
    status: Option<String>,
    #[serde(default = "default_limit")]
    limit: usize,
}

#[derive(Debug, Deserialize)]
struct CreateJob {
    library_id: Option<String>,
    #[serde(alias = "profile_id")]
    policy_id: Option<String>,
    scope: Option<Scope>,
    policy: Option<Policy>,
    options: Option<serde_json::Value>,
}

async fn list(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ListQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let limit = query.limit.clamp(1, 200);
    let connection = state.database.connect()?;
    let status = query.status.filter(|value| value != "all");
    let mut rows = if let Some(before) = query.before_id {
        query_json(
            &connection,
            "SELECT * FROM jobs WHERE (queued_at,id)<(SELECT queued_at,id FROM jobs WHERE id=?) AND (? IS NULL OR status=?) ORDER BY queued_at DESC,id DESC LIMIT ?",
            params![before, status, status, limit as i64],
        )?
    } else {
        query_json(
            &connection,
            "SELECT * FROM jobs WHERE (? IS NULL OR status=?) ORDER BY queued_at DESC,id DESC LIMIT ?",
            params![status, status, limit as i64],
        )?
    };
    rows.iter_mut().for_each(|value| {
        decode_json_field(value, "policy_json", "policy");
        decode_json_field(value, "action_json", "action");
    });
    let next = (rows.len() == limit)
        .then(|| {
            rows.last()
                .and_then(|row| row.get("id"))
                .and_then(|id| id.as_str())
                .map(str::to_owned)
        })
        .flatten();
    Ok(Json(serde_json::json!({"rows":rows,"next":next})))
}

async fn create(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<CreateJob>,
) -> Result<(axum::http::StatusCode, Json<serde_json::Value>), ApiError> {
    if let Some(error) = state.runtime_error.read().await.clone() {
        return Err(ApiError::unavailable(anyhow::anyhow!(
            "conversion runtime unavailable: {error}"
        )));
    }
    let connection = state.database.connect()?;
    let library_id = match payload.library_id {
        Some(id) => id,
        None => connection
            .query_row(
                "SELECT id FROM libraries WHERE enabled=1 ORDER BY created_at,id LIMIT 1",
                [],
                |row| row.get(0),
            )
            .optional()?
            .ok_or_else(|| {
                ApiError::bad_request(anyhow::anyhow!("no enabled library is configured"))
            })?,
    };
    let (policy_id, stored): (String, String) = match payload.policy_id {
        Some(id) => connection
            .query_row(
                "SELECT id,policy_json FROM policies WHERE id=?",
                [&id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?
            .ok_or_else(|| ApiError::bad_request(anyhow::anyhow!("conversion policy not found")))?,
        None => connection.query_row(
            "SELECT id,policy_json FROM policies ORDER BY is_default DESC,created_at,id LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?,
    };
    let mut policy = if let Some(policy) = payload.policy {
        policy
    } else if let Some(options) = payload.options {
        policy_from_options(&options)
    } else {
        serde_json::from_str(&stored)?
    };
    if let Some(scope) = payload.scope {
        policy.scope = scope;
    }
    policy = policy.normalize(state.config.cpu_limit);
    let id = state.database.enqueue(
        &library_id,
        Some(&policy_id),
        &policy,
        JobSource::Manual,
        None,
    )?;
    Ok((
        axum::http::StatusCode::CREATED,
        Json(serde_json::json!({"id":id,"job_id":id,"status":"queued"})),
    ))
}

fn policy_from_options(value: &serde_json::Value) -> Policy {
    let strategy = if value
        .get("integrity_audit")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        Strategy::Audit
    } else if value
        .get("no_convert")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        Strategy::Recheck
    } else if value
        .get("smart_distance")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        Strategy::Smart
    } else if value.get("distance").is_some_and(|v| !v.is_null()) {
        Strategy::Lossy
    } else {
        Strategy::Lossless
    };
    let scope = if value
        .get("failed_only")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        Scope::Failed
    } else if value
        .get("recheck_all")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        Scope::Library
    } else {
        Scope::Unprocessed
    };
    Policy {
        strategy,
        scope,
        effort: value.get("effort").and_then(|v| v.as_u64()).unwrap_or(10) as u8,
        threads: value.get("threads").and_then(|v| v.as_u64()).unwrap_or(4) as usize,
        distance: value
            .get("distance")
            .and_then(|v| v.as_f64())
            .map(|v| v as f32),
        smart_mb: value
            .get("smart_mb")
            .and_then(|v| v.as_f64())
            .unwrap_or(10.0) as f32,
        smart_mp: value
            .get("smart_mp")
            .and_then(|v| v.as_f64())
            .unwrap_or(5.0) as f32,
        backup: value
            .get("backup")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
    }
}

#[derive(Debug, Deserialize)]
struct DetailQuery {
    before_outcome: Option<i64>,
    #[serde(default = "default_limit")]
    limit: usize,
}

#[derive(Debug, Deserialize)]
struct PageDecisionQuery {
    path: Option<String>,
    #[serde(default = "default_page_limit")]
    limit: usize,
}

type RetryJob = (
    String,
    Option<String>,
    String,
    Option<String>,
    Option<String>,
);

async fn page_decisions(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Query(query): Query<PageDecisionQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let connection = state.database.connect()?;
    if connection
        .query_row("SELECT 1 FROM jobs WHERE id=?", [&id], |_| Ok(()))
        .optional()?
        .is_none()
    {
        return Err(ApiError::not_found(anyhow::anyhow!("job not found")));
    }
    let rows = query_json(
        &connection,
        "SELECT p.*,o.job_id,o.path FROM page_outcomes p JOIN job_archive_outcomes o ON o.id=p.job_outcome_id WHERE o.job_id=? AND (? IS NULL OR o.path=?) ORDER BY o.path,p.entry_path LIMIT ?",
        params![
            id,
            query.path,
            query.path,
            query.limit.clamp(1, 5000) as i64
        ],
    )?;
    let mut rows = rows;
    rows.iter_mut()
        .for_each(|row| decode_json_field(row, "attempts_json", "attempts"));
    Ok(Json(serde_json::json!({"rows":rows})))
}

async fn detail(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Query(query): Query<DetailQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let connection = state.database.connect()?;
    let mut job = query_json(&connection, "SELECT * FROM jobs WHERE id=?", [&id])?
        .pop()
        .ok_or_else(|| ApiError::not_found(anyhow::anyhow!("job not found")))?;
    decode_json_field(&mut job, "policy_json", "policy");
    decode_json_field(&mut job, "action_json", "action");
    if let Some(latest) = state.progress.latest(&id)
        && let (Some(job), serde_json::Value::Object(progress)) = (job.as_object_mut(), latest.data)
    {
        job.extend(progress);
        job.insert("current_stage".into(), latest.event.into());
        job.insert("progress_at".into(), latest.at.into());
    }
    let limit = query.limit.clamp(1, 500);
    let outcomes = query_json(
        &connection,
        "SELECT * FROM job_archive_outcomes WHERE job_id=? AND id<? ORDER BY id DESC LIMIT ?",
        params![id, query.before_outcome.unwrap_or(i64::MAX), limit as i64],
    )?;
    let events = query_json(
        &connection,
        "SELECT * FROM job_events WHERE job_id=? ORDER BY id DESC LIMIT 100",
        [&id],
    )?;
    let next = (outcomes.len() == limit)
        .then(|| {
            outcomes
                .last()
                .and_then(|row| row.get("id"))
                .and_then(|id| id.as_i64())
        })
        .flatten();
    Ok(Json(
        serde_json::json!({"job":job,"outcomes":outcomes,"events":events,"next_outcome":next}),
    ))
}

async fn live(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    detail(
        State(state),
        Path(id),
        Query(DetailQuery {
            before_outcome: None,
            limit: 100,
        }),
    )
    .await
}

async fn history(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Query(query): Query<DetailQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let Json(value) = detail(
        State(state.clone()),
        Path(id.clone()),
        Query(DetailQuery {
            before_outcome: query.before_outcome,
            limit: query.limit,
        }),
    )
    .await?;
    let job = value.get("job").cloned().unwrap_or_default();
    let outcomes = value
        .get("outcomes")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    let connection = state.database.connect()?;
    let grouped = query_json(
        &connection,
        "SELECT outcome,COUNT(*) AS count FROM job_archive_outcomes WHERE job_id=? GROUP BY outcome",
        [&id],
    )?;
    let mut counts = serde_json::Map::from_iter([
        ("all".into(), 0.into()),
        ("optimized".into(), 0.into()),
        ("already_current".into(), 0.into()),
        ("retained".into(), 0.into()),
        ("failed".into(), 0.into()),
    ]);
    for row in grouped {
        let raw = row
            .get("outcome")
            .and_then(|value| value.as_str())
            .unwrap_or("failed");
        let outcome = history_outcome(raw);
        let count = row
            .get("count")
            .and_then(|value| value.as_i64())
            .unwrap_or(0);
        let current = counts
            .get(outcome)
            .and_then(|value| value.as_i64())
            .unwrap_or(0);
        counts.insert(outcome.into(), (current + count).into());
        let all = counts
            .get("all")
            .and_then(|value| value.as_i64())
            .unwrap_or(0);
        counts.insert("all".into(), (all + count).into());
    }
    let entries=outcomes.into_iter().map(|row|{
        let raw=row.get("outcome").and_then(|value|value.as_str()).unwrap_or("failed");
        let outcome=history_outcome(raw);
        serde_json::json!({"id":row.get("id"),"archive_id":row.get("archive_id"),"outcome":outcome,"label":outcome.replace('_'," "),"path":row.get("path"),"recorded_at":row.get("created_at"),"detail":row.get("error_message").or_else(||row.get("detail")).cloned().unwrap_or_else(||"Archive processing completed.".into()),"converted_pages":row.get("converted_pages"),"lossless_pages":row.get("lossless_pages"),"lossy_pages":row.get("lossy_pages"),"retained_pages":row.get("retained_pages"),"bytes_saved":row.get("bytes_saved"),"jxl_effort":row.get("effort")})
    }).collect::<Vec<_>>();
    Ok(Json(
        serde_json::json!({"job":job,"history_entries":entries,"outcome_counts":counts,"events":value.get("events").cloned().unwrap_or_default(),"next_outcome":value.get("next_outcome").cloned().unwrap_or_default()}),
    ))
}

fn history_outcome(raw: &str) -> &'static str {
    match raw {
        "optimized" | "converted" | "corrected" | "repaired" => "optimized",
        "already_current" | "skipped" => "already_current",
        "retained" | "unchanged" | "unsupported" | "audited" => "retained",
        _ => "failed",
    }
}

async fn cancel(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    if !state.database.cancel_job(&id)? {
        return Err(ApiError::conflict(anyhow::anyhow!(
            "job is not cancellable"
        )));
    }
    Ok(Json(serde_json::json!({"status":"stopping"})))
}

async fn retry(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let connection = state.database.connect()?;
    let previous: Option<RetryJob> = connection
        .query_row(
            "SELECT library_id,policy_id,policy_json,only_path,action_json FROM jobs WHERE id=?",
            [&id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .optional()?;
    let (library, policy_id, policy, only_path, action) =
        previous.ok_or_else(|| ApiError::not_found(anyhow::anyhow!("job not found")))?;
    let policy: Policy = serde_json::from_str(&policy)?;
    let new_id = if let Some(action) = action {
        let path = only_path.as_deref().ok_or_else(|| {
            ApiError::conflict(anyhow::anyhow!("repair retry is missing its archive path"))
        })?;
        state.database.enqueue_repair(
            &library,
            policy_id.as_deref(),
            &policy,
            path,
            &serde_json::from_str(&action)?,
        )?
    } else {
        state.database.enqueue(
            &library,
            policy_id.as_deref(),
            &policy,
            JobSource::Retry,
            only_path.as_deref(),
        )?
    };
    Ok(Json(serde_json::json!({"id":new_id,"status":"queued"})))
}

async fn events(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let poll_id = id.clone();
    let seen = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let broadcast_seen = seen.clone();
    let stream = BroadcastStream::new(state.progress.subscribe()).filter_map(move |event| {
        let id = id.clone();
        let seen = broadcast_seen.clone();
        async move {
            match event {
                Ok(event) if event.job_id == id => {
                    seen.store(event.sequence, std::sync::atomic::Ordering::Relaxed);
                    Some(Ok(Event::default()
                        .id(event.sequence.to_string())
                        .event(event.event.clone())
                        .json_data(event)
                        .expect("serializable progress event")))
                }
                _ => None,
            }
        }
    });
    let progress = state.progress.clone();
    let poll_seen = seen.clone();
    let polling =
        tokio_stream::wrappers::IntervalStream::new(tokio::time::interval(Duration::from_secs(1)))
            .filter_map(move |_| {
                let latest = progress.latest(&poll_id);
                let previous = poll_seen.load(std::sync::atomic::Ordering::Relaxed);
                let seen = poll_seen.clone();
                async move {
                    latest
                        .filter(|event| event.sequence > previous)
                        .map(|event| {
                            seen.store(event.sequence, std::sync::atomic::Ordering::Relaxed);
                            Ok(Event::default()
                                .id(event.sequence.to_string())
                                .event(event.event.clone())
                                .json_data(event)
                                .expect("serializable progress event"))
                        })
                }
            });
    Sse::new(futures::stream::select(stream, polling))
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}

const fn default_limit() -> usize {
    100
}

const fn default_page_limit() -> usize {
    500
}
