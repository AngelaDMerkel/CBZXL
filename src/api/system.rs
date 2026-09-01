use super::{ApiError, AppState, decode_json_field, query_json};
use crate::{VERSION, progress::ProgressHub};
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::State,
    http::{HeaderValue, StatusCode, header},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use rusqlite::OptionalExtension;
use serde::Deserialize;
use serde::Serialize;
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, LazyLock},
    time::Instant,
};
use tokio::io::AsyncReadExt;

static STATISTICS_CACHE: LazyLock<
    tokio::sync::Mutex<HashMap<PathBuf, (Instant, serde_json::Value)>>,
> = LazyLock::new(|| tokio::sync::Mutex::new(HashMap::new()));
static DASHBOARD_CACHE: LazyLock<
    tokio::sync::Mutex<HashMap<PathBuf, (Instant, serde_json::Value)>>,
> = LazyLock::new(|| tokio::sync::Mutex::new(HashMap::new()));

#[derive(Debug, Serialize)]
struct Health {
    status: &'static str,
    version: &'static str,
    database: &'static str,
    conversion_runtime: &'static str,
    error: Option<String>,
}

pub fn public_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/api/docs", get(api_docs))
        .route("/api/openapi.json", get(openapi))
        .route("/api/integrations/homepage", get(homepage))
        .route("/api/v2/integrations/homepage", get(homepage))
}

async fn api_docs() -> Html<&'static str> {
    Html(
        r#"<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>CBZXL API</title><style>body{max-width:760px;margin:48px auto;padding:0 24px;font:16px system-ui;color:#172033}code,a{color:#2563eb}li{margin:9px 0}</style></head><body><h1>CBZXL API</h1><p>The machine-readable OpenAPI document is available at <a href="/api/openapi.json"><code>/api/openapi.json</code></a>.</p><h2>Primary endpoints</h2><ul><li><code>GET /health</code> and <code>GET /ready</code></li><li><code>GET /api/v2/dashboard</code></li><li><code>GET|POST /api/v2/jobs</code></li><li><code>GET /api/v2/archives</code> and <code>/issues</code></li><li><code>GET|POST /api/v2/policies</code> and <code>/schedules</code></li><li><code>GET /api/v2/database/export</code></li></ul></body></html>"#,
    )
}

async fn openapi() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "openapi":"3.1.0","info":{"title":"CBZXL API","version":VERSION},
        "paths":{
            "/api/v2/bootstrap":{"get":{}},"/api/v2/dashboard":{"get":{}},"/api/v2/statistics":{"get":{}},
            "/api/v2/jobs":{"get":{},"post":{}},"/api/v2/jobs/{id}":{"get":{}},
            "/api/v2/jobs/{id}/page-decisions":{"get":{}},"/api/v2/issues":{"get":{}},
            "/api/v2/issues/{id}/diagnostics":{"get":{}},"/api/v2/archives":{"get":{}},
            "/api/v2/schedules":{"get":{},"post":{}},"/api/v2/policies":{"get":{},"post":{}},
            "/api/v2/database/export":{"get":{}},"/api/v2/integrations/homepage":{"get":{}},
            "/health":{"get":{}},"/ready":{"get":{}}
        }
    }))
}

pub fn protected_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/bootstrap", get(bootstrap))
        .route("/dashboard", get(dashboard))
        .route("/statistics", get(statistics))
        .route("/database/export", get(export_database))
        .route("/database/import", post(import_database))
        .route("/database/imports", get(imports))
}

async fn statistics(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let mut cache = STATISTICS_CACHE.lock().await;
    if let Some((created, value)) = cache.get(state.database.path())
        && created.elapsed() < state.config.statistics_cache
    {
        return Ok(Json(value.clone()));
    }
    let database = state.database.clone();
    let value = tokio::task::spawn_blocking(move || calculate_statistics(&database)).await??;
    cache.insert(
        state.database.path().to_owned(),
        (Instant::now(), value.clone()),
    );
    Ok(Json(value))
}

fn calculate_statistics(database: &crate::db::Database) -> anyhow::Result<serde_json::Value> {
    let connection = database.connect()?;
    let library = query_json(&connection, "SELECT COUNT(*) AS archives,COALESCE(SUM(baseline_size),0) AS baseline_bytes,COALESCE(SUM(current_size),0) AS current_bytes,COALESCE(SUM(bytes_saved),0) AS net_bytes,COALESCE(SUM(image_count),0) AS pages,COALESCE(SUM(converted_pages),0) AS converted_pages,COALESCE(SUM(retained_pages),0) AS retained_pages,COALESCE(SUM(CASE WHEN status='processed' AND bytes_saved>0 THEN 1 ELSE 0 END),0) AS optimized_archives,COALESCE(AVG(CASE WHEN status='processed' AND bytes_saved>0 THEN percent_saved END),0) AS average_percent_saved,COALESCE(AVG(CASE WHEN status='processed' AND bytes_saved>0 THEN duration_ms END),0) AS average_duration_ms FROM archives WHERE status!='missing'",[])?.pop().unwrap_or_default();
    let archive_outcomes = query_json(
        &connection,
        "SELECT outcome,COUNT(*) AS archives,COALESCE(SUM(converted_pages),0) AS converted_pages,COALESCE(SUM(retained_pages),0) AS retained_pages,COALESCE(SUM(failed_pages),0) AS failed_pages,COALESCE(SUM(bytes_saved),0) AS bytes_saved FROM job_archive_outcomes GROUP BY outcome ORDER BY archives DESC,outcome",
        [],
    )?;
    let inventory = query_json(
        &connection,
        "SELECT f.format,SUM(f.file_count) AS files,SUM(f.compressed_bytes) AS compressed_bytes,SUM(f.uncompressed_bytes) AS uncompressed_bytes FROM archive_formats f JOIN archives a ON a.id=f.archive_id WHERE a.status!='missing' GROUP BY f.format ORDER BY files DESC,f.format",
        [],
    )?;
    let inventoried_archives: i64 = connection.query_row(
        "SELECT COUNT(DISTINCT f.archive_id) FROM archive_formats f JOIN archives a ON a.id=f.archive_id WHERE a.status!='missing'",
        [],
        |row| row.get(0),
    )?;
    let policies = query_json(
        &connection,
        "SELECT COALESCE(p.name,'Deleted policy') AS policy,COUNT(o.id) AS attempted,SUM(CASE WHEN o.outcome IN ('optimized','corrected','repaired') THEN 1 ELSE 0 END) AS optimized,COALESCE(AVG(CASE WHEN o.outcome IN ('optimized','corrected','repaired') AND o.source_size>0 THEN o.bytes_saved*100.0/o.source_size END),0) AS average_percent_saved,COALESCE(AVG(o.duration_ms),0) AS average_duration_ms FROM job_archive_outcomes o JOIN jobs j ON j.id=o.job_id LEFT JOIN policies p ON p.id=j.policy_id WHERE o.outcome IN ('optimized','corrected','repaired','retained','failed') GROUP BY p.name ORDER BY attempted DESC,policy",
        [],
    )?;
    let issues = query_json(
        &connection,
        "SELECT kind,stage,COUNT(*) AS issues,COUNT(DISTINCT archive_id) AS archives FROM issues WHERE status IN ('open','queued') GROUP BY kind,stage ORDER BY issues DESC",
        [],
    )?;
    let page_outcomes = query_json(
        &connection,
        "SELECT CASE outcome WHEN 'already_current' THEN 'Already current' WHEN 'extension_corrected' THEN 'Extension corrected' WHEN 'converted' THEN 'Converted' WHEN 'retained' THEN 'Retained' WHEN 'failed' THEN 'Failed' WHEN 'preserved' THEN 'Preserved' WHEN 'unsupported' THEN 'Unsupported' ELSE replace(outcome,'_',' ') END AS label,COUNT(*) AS count FROM page_outcomes GROUP BY label ORDER BY count DESC,label",
        [],
    )?;
    let format_effectiveness = query_json(
        &connection,
        "SELECT CASE lower(COALESCE(source_format,'')) WHEN 'image/jpeg' THEN 'JPEG' WHEN 'jpeg' THEN 'JPEG' WHEN 'jpg' THEN 'JPEG' WHEN 'image/png' THEN 'PNG' WHEN 'png' THEN 'PNG' WHEN 'image/webp' THEN 'WebP' WHEN 'webp' THEN 'WebP' WHEN 'image/gif' THEN 'GIF' WHEN 'gif' THEN 'GIF' WHEN 'image/jxl' THEN 'JPEG XL' WHEN 'jpeg xl' THEN 'JPEG XL' WHEN 'jxl' THEN 'JPEG XL' WHEN 'image/tiff' THEN 'TIFF' WHEN 'tiff' THEN 'TIFF' WHEN 'image/bmp' THEN 'BMP' WHEN 'bmp' THEN 'BMP' WHEN 'inode/x-empty' THEN 'Empty file' WHEN 'application/octet-stream' THEN 'Unknown' WHEN '' THEN 'Unknown' ELSE source_format END AS label,COUNT(*) AS count,COALESCE(SUM(COALESCE(source_size,0)-COALESCE(final_size,source_size,0)),0) AS net_bytes,COALESCE(AVG(CASE WHEN source_size>0 AND final_size<source_size THEN (source_size-final_size)*100.0/source_size END),0) AS average_percent_saved FROM page_outcomes GROUP BY label ORDER BY count DESC,label",
        [],
    )?;
    let run_trend = query_json(
        &connection,
        "SELECT s.job_id AS id,substr(s.recorded_at,1,10) AS date,s.net_bytes/1073741824.0 AS cumulative_net_gb,s.average_percent_saved FROM library_snapshots s JOIN jobs j ON j.id=s.job_id WHERE j.status IN ('succeeded','partial') ORDER BY s.recorded_at,s.job_id",
        [],
    )?;
    let effort = query_json(&connection,"SELECT COUNT(DISTINCT CASE WHEN j.status IN ('succeeded','partial','failed','cancelled','interrupted') THEN j.id END) AS completed_runs,COUNT(CASE WHEN o.outcome NOT IN ('already_current','deferred') THEN 1 END) AS archives_attempted,COALESCE(SUM(CASE WHEN o.outcome NOT IN ('already_current','deferred') THEN o.image_count ELSE 0 END),0) AS pages_examined,COALESCE(SUM(CASE WHEN o.outcome NOT IN ('already_current','deferred') THEN o.duration_ms ELSE 0 END),0)/3600000.0 AS encoder_hours FROM jobs j LEFT JOIN job_archive_outcomes o ON o.job_id=j.id",[])?.pop().unwrap_or_default();
    let mut savings = {
        let mut statement=connection.prepare("SELECT percent_saved FROM archives WHERE status='processed' AND bytes_saved>0 ORDER BY percent_saved")?;
        statement
            .query_map([], |row| row.get::<_, f64>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?
    };
    let durations = {
        let mut statement=connection.prepare("SELECT duration_ms/1000.0 FROM job_archive_outcomes WHERE duration_ms>0 AND outcome IN ('optimized','corrected','repaired','retained','failed') ORDER BY duration_ms")?;
        statement
            .query_map([], |row| row.get::<_, f64>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?
    };
    let percentile = |values: &[f64], fraction: f64| -> f64 {
        if values.is_empty() {
            0.0
        } else {
            values[((values.len() - 1) as f64 * fraction).round() as usize]
        }
    };
    let savings_distribution = [
        ("0–10%",0.0,10.0),("10–20%",10.0,20.0),("20–30%",20.0,30.0),
        ("30–40%",30.0,40.0),("40–50%",40.0,50.0),("50%+",50.0,f64::INFINITY),
    ].into_iter().map(|(label,min,max)|serde_json::json!({"label":label,"count":savings.iter().filter(|value|**value>=min&&**value<max).count()})).collect::<Vec<_>>();
    let processing_distribution = [
        ("<10s",0.0,10.0),("10–30s",10.0,30.0),("30–60s",30.0,60.0),
        ("1–5m",60.0,300.0),("5m+",300.0,f64::INFINITY),
    ].into_iter().map(|(label,min,max)|serde_json::json!({"label":label,"count":durations.iter().filter(|value|**value>=min&&**value<max).count()})).collect::<Vec<_>>();
    let archives = library
        .get("archives")
        .and_then(|value| value.as_i64())
        .unwrap_or(0);
    let pages = library
        .get("pages")
        .and_then(|value| value.as_i64())
        .unwrap_or(0);
    let converted = library
        .get("converted_pages")
        .and_then(|value| value.as_i64())
        .unwrap_or(0);
    let optimized = library
        .get("optimized_archives")
        .and_then(|value| value.as_i64())
        .unwrap_or(0);
    let attempted = archive_outcomes
        .iter()
        .filter(|row| {
            row.get("outcome")
                .and_then(|value| value.as_str())
                .is_some_and(|outcome| {
                    matches!(
                        outcome,
                        "optimized" | "corrected" | "repaired" | "retained" | "failed"
                    )
                })
        })
        .filter_map(|row| row.get("archives").and_then(|value| value.as_i64()))
        .sum::<i64>();
    let accepted = archive_outcomes
        .iter()
        .filter(|row| {
            row.get("outcome")
                .and_then(|value| value.as_str())
                .is_some_and(|outcome| matches!(outcome, "optimized" | "corrected" | "repaired"))
        })
        .filter_map(|row| row.get("archives").and_then(|value| value.as_i64()))
        .sum::<i64>();
    let retained = library
        .get("retained_pages")
        .and_then(|value| value.as_i64())
        .unwrap_or(0);
    let file_rows=inventory.iter().map(|row|serde_json::json!({"label":row.get("format"),"file_count":row.get("files"),"storage_bytes":row.get("compressed_bytes")})).collect::<Vec<_>>();
    let file_count = inventory
        .iter()
        .filter_map(|row| row.get("files").and_then(|value| value.as_i64()))
        .sum::<i64>();
    let storage = inventory
        .iter()
        .filter_map(|row| row.get("compressed_bytes").and_then(|value| value.as_i64()))
        .sum::<i64>();
    let jxl_files = inventory
        .iter()
        .filter(|row| row.get("format").and_then(|value| value.as_str()) == Some("JPEG XL"))
        .filter_map(|row| row.get("files").and_then(|value| value.as_i64()))
        .sum::<i64>();
    let processing_policies=policies.iter().map(|row|serde_json::json!({
        "name":row.get("policy"),"median_seconds":row.get("average_duration_ms").and_then(|value|value.as_f64()).unwrap_or(0.0)/1000.0,
        "average_percent_saved":row.get("average_percent_saved"),
        "success_percent":if row.get("attempted").and_then(|value|value.as_i64()).unwrap_or(0)>0{row.get("optimized").and_then(|value|value.as_f64()).unwrap_or(0.0)*100.0/row.get("attempted").and_then(|value|value.as_f64()).unwrap_or(1.0)}else{0.0}
    })).collect::<Vec<_>>();
    let attention=issues.iter().map(|row|serde_json::json!({"id":format!("{}:{}",row.get("kind").and_then(|v|v.as_str()).unwrap_or("issue"),row.get("stage").and_then(|v|v.as_str()).unwrap_or("unknown")),"label":format!("{} · {}",row.get("kind").and_then(|v|v.as_str()).unwrap_or("issue").replace('_'," "),row.get("stage").and_then(|v|v.as_str()).unwrap_or("unknown").replace('_'," ")),"count":row.get("issues")})).collect::<Vec<_>>();
    let average = if savings.is_empty() {
        0.0
    } else {
        savings.iter().sum::<f64>() / savings.len() as f64
    };
    let library_compat = serde_json::json!({
        "archives":archives,"optimized_archives":optimized,"pages":pages,
        "source_bytes":library.get("baseline_bytes"),"current_bytes":library.get("current_bytes"),
        "jxl_coverage_percent":if file_count>0{jxl_files as f64*100.0/file_count as f64}else if pages>0{converted as f64*100.0/pages as f64}else{0.0}
    });
    savings.shrink_to_fit();
    Ok(serde_json::json!({
        "library":library_compat,
        "health":{"library_coverage_percent":if archives>0{optimized as f64*100.0/archives as f64}else{0.0},"average_percent_saved":average,"retained_pages":retained,"success_percent":if attempted>0{accepted as f64*100.0/attempted as f64}else{0.0}},
        "run_trend":run_trend,
        "filetypes":{"rows":file_rows,"total_files":file_count,"total_storage_bytes":storage,"inventoried_archives":inventoried_archives,"archive_count":archives},
        "formats":format_effectiveness,"outcomes":page_outcomes,
        "savings_distribution":savings_distribution,"savings_summary":{"median":percentile(&savings,0.5),"average":average,"middle_50":[percentile(&savings,0.25),percentile(&savings,0.75)]},
        "processing":{"median_seconds":percentile(&durations,0.5),"p90_seconds":percentile(&durations,0.9),"distribution":processing_distribution,"policies":processing_policies},
        "attention":attention,"effort":effort,
        "archive_outcomes":archive_outcomes,"inventory":inventory,"policies":policies,"issues":issues
    }))
}

async fn export_database(State(state): State<Arc<AppState>>) -> Result<Response, ApiError> {
    let database = state.database.clone();
    let (temporary, reader, size) =
        tokio::task::spawn_blocking(move || database.export_file()).await??;
    let reader = tokio::fs::File::from_std(reader);
    let stream =
        futures::stream::try_unfold((reader, temporary), |(mut reader, temporary)| async move {
            let mut buffer = vec![0_u8; 64 * 1024];
            let count = reader.read(&mut buffer).await?;
            if count == 0 {
                return Ok::<_, std::io::Error>(None);
            }
            buffer.truncate(count);
            Ok::<_, std::io::Error>(Some((Bytes::from(buffer), (reader, temporary))))
        });
    let mut response = Response::new(Body::from_stream(stream));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/vnd.sqlite3"),
    );
    response.headers_mut().insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_static("attachment; filename=cbzxl-v5.sqlite3"),
    );
    response.headers_mut().insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&size.to_string())?,
    );
    Ok(response)
}

#[derive(Deserialize)]
struct ImportInput {
    path: String,
    #[serde(default)]
    dry_run: bool,
}

async fn import_database(
    State(state): State<Arc<AppState>>,
    Json(input): Json<ImportInput>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let database = state.database.clone();
    let path = std::path::PathBuf::from(input.path);
    let report = tokio::task::spawn_blocking(move || database.import(&path, input.dry_run))
        .await?
        .map_err(ApiError::bad_request)?;
    Ok(Json(serde_json::to_value(report)?))
}

async fn imports(State(state): State<Arc<AppState>>) -> Result<Json<serde_json::Value>, ApiError> {
    let rows = query_json(
        &state.database.connect()?,
        "SELECT * FROM imports ORDER BY started_at DESC",
        [],
    )?;
    Ok(Json(serde_json::json!({"rows":rows})))
}

async fn health(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let runtime = state.runtime_error.read().await.clone();
    let database_error = state.database_error.read().await.clone();
    if let Some(error) = database_error {
        return Json(Health {
            status: "degraded",
            version: VERSION,
            database: "unavailable",
            conversion_runtime: if runtime.is_some() {
                "unavailable"
            } else {
                "ok"
            },
            error: Some(error),
        });
    }
    match state.database.connect().and_then(|connection| {
        connection
            .query_row("SELECT 1", [], |_| Ok(()))
            .map_err(Into::into)
    }) {
        Ok(_) => Json(Health {
            status: if runtime.is_some() { "degraded" } else { "ok" },
            version: VERSION,
            database: "ok",
            conversion_runtime: if runtime.is_some() {
                "unavailable"
            } else {
                "ok"
            },
            error: runtime,
        }),
        Err(error) => Json(Health {
            status: "degraded",
            version: VERSION,
            database: "unavailable",
            conversion_runtime: if runtime.is_some() {
                "unavailable"
            } else {
                "ok"
            },
            error: Some(error.to_string()),
        }),
    }
}

async fn ready(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Health>, (StatusCode, Json<Health>)> {
    let runtime = state.runtime_error.read().await.clone();
    if let Some(error) = state.database_error.read().await.clone() {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(Health {
                status: "unavailable",
                version: VERSION,
                database: "unavailable",
                conversion_runtime: if runtime.is_some() {
                    "unavailable"
                } else {
                    "ok"
                },
                error: Some(error),
            }),
        ));
    }
    if let Err(error) = state.database.connect().and_then(|connection| {
        connection
            .query_row("SELECT 1", [], |_| Ok(()))
            .map_err(Into::into)
    }) {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(Health {
                status: "unavailable",
                version: VERSION,
                database: "unavailable",
                conversion_runtime: if runtime.is_some() {
                    "unavailable"
                } else {
                    "ok"
                },
                error: Some(error.to_string()),
            }),
        ));
    }
    if let Some(error) = runtime {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(Health {
                status: "unavailable",
                version: VERSION,
                database: "ok",
                conversion_runtime: "unavailable",
                error: Some(error),
            }),
        ));
    }
    if let Ok(Some(error)) = worker_stall_error(&state) {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(Health {
                status: "unavailable",
                version: VERSION,
                database: "ok",
                conversion_runtime: "unavailable",
                error: Some(error),
            }),
        ));
    }
    Ok(Json(Health {
        status: "ready",
        version: VERSION,
        database: "ok",
        conversion_runtime: "ok",
        error: None,
    }))
}

fn worker_stall_error(state: &AppState) -> anyhow::Result<Option<String>> {
    if state.config.worker_stall.is_zero() {
        return Ok(None);
    }
    let connection = state.database.connect()?;
    let active: Option<(String, Option<String>)> = connection
        .query_row(
            "SELECT id,started_at FROM jobs WHERE status IN ('running','stopping') ORDER BY started_at,id LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((id, started_at)) = active else {
        return Ok(None);
    };
    let timestamp = state
        .progress
        .latest(&id)
        .map(|event| event.at)
        .or(started_at);
    let Some(timestamp) = timestamp else {
        return Ok(None);
    };
    let timestamp =
        time::OffsetDateTime::parse(&timestamp, &time::format_description::well_known::Rfc3339)?;
    let silent = time::OffsetDateTime::now_utc() - timestamp;
    Ok(
        (silent.whole_seconds() >= state.config.worker_stall.as_secs() as i64).then(|| {
            format!(
                "Worker has reported no progress for {} seconds while job {id} is active",
                silent.whole_seconds()
            )
        }),
    )
}

async fn bootstrap(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let runtime_error = state.runtime_error.read().await.clone();
    if let Some(error) = state.database_error.read().await.clone() {
        return Ok(Json(serde_json::json!({
            "version":VERSION,"libraries":[],"policies":[],"schedules":[],
            "capabilities":{"cpu_limit":state.config.cpu_limit},"authentication":state.config.api_token.is_some(),
            "timezone":state.config.timezone,"database_status":"unavailable","database_error":error,
            "conversion_runtime":if runtime_error.is_some(){"unavailable"}else{"ok"},"conversion_runtime_error":runtime_error
        })));
    }
    let connection = state.database.connect()?;
    let mut policies = query_json(
        &connection,
        "SELECT * FROM policies ORDER BY is_default DESC,name",
        [],
    )?;
    policies.iter_mut().for_each(|value| {
        decode_json_field(value, "policy_json", "policy");
        if let Some(policy) = value.get("policy").cloned()
            && let Some(object) = value.as_object_mut()
        {
            object.insert("options".into(), legacy_options(&policy));
            if let Some(default) = object.get("is_default").and_then(|value| value.as_i64()) {
                object.insert("is_default".into(), (default != 0).into());
            }
        }
    });
    let mut schedules = query_json(
        &connection,
        "SELECT * FROM schedules ORDER BY created_at,id",
        [],
    )?;
    schedules.iter_mut().for_each(|value| {
        decode_json_field(value, "cadence_json", "cadence_options");
        let cadence = value.get("cadence_options").cloned().unwrap_or_default();
        if let Some(object) = value.as_object_mut() {
            if let Some(policy) = object.get("policy_id").cloned() {
                object.insert("profile_id".into(), policy);
            }
            if let Some(values) = cadence.as_object() {
                for (key, value) in values {
                    if key != "cadence" {
                        object.insert(key.clone(), value.clone());
                    }
                }
            }
        }
    });
    let mut libraries = serde_json::to_value(state.database.libraries()?)?;
    if let Some(rows) = libraries.as_array_mut() {
        for row in rows {
            if let Some(object) = row.as_object_mut()
                && let Some(root) = object.get("root").cloned()
            {
                object.insert("root_path".into(), root);
            }
        }
    }
    Ok(Json(serde_json::json!({
        "version": VERSION,
        "libraries": libraries,
        "policies": policies.clone(),
        "profiles": policies,
        "schedules": schedules,
        "capabilities": {"cpu_limit":state.config.cpu_limit},
        "authentication": state.config.api_token.is_some(),
        "timezone": state.config.timezone,
        "conversion_runtime": if runtime_error.is_some(){"unavailable"}else{"ok"},
        "conversion_runtime_error": runtime_error,
        "database_status":"ok",
        "database_error":serde_json::Value::Null,
    })))
}

fn legacy_options(policy: &serde_json::Value) -> serde_json::Value {
    let strategy = policy
        .get("strategy")
        .and_then(|value| value.as_str())
        .unwrap_or("lossless");
    let scope = policy
        .get("scope")
        .and_then(|value| value.as_str())
        .unwrap_or("unprocessed");
    serde_json::json!({
        "effort":policy.get("effort"),"threads":policy.get("threads"),"backup":policy.get("backup"),
        "distance":if matches!(strategy,"smart"|"lossy"){policy.get("distance").cloned().unwrap_or(0.5.into())}else{serde_json::Value::Null},
        "smart_mb":policy.get("smart_mb"),"smart_mp":policy.get("smart_mp"),
        "smart_distance":strategy=="smart","no_convert":matches!(strategy,"recheck"|"audit"),
        "integrity_audit":strategy=="audit","recheck_all":strategy=="audit"||scope=="library","failed_only":strategy!="audit"&&scope=="failed"
    })
}

async fn dashboard(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let mut cache = DASHBOARD_CACHE.lock().await;
    if let Some((created, value)) = cache.get(state.database.path())
        && created.elapsed() < std::time::Duration::from_secs(1)
    {
        return Ok(Json(value.clone()));
    }
    let database = state.database.clone();
    let value = tokio::task::spawn_blocking(move || dashboard_value(&database)).await??;
    cache.insert(
        state.database.path().to_owned(),
        (Instant::now(), value.clone()),
    );
    Ok(Json(value))
}

fn dashboard_value(database: &crate::db::Database) -> anyhow::Result<serde_json::Value> {
    let connection = database.connect()?;
    let mut totals = query_json(&connection, "SELECT COUNT(*) AS archives,COALESCE(SUM(CASE WHEN status='processed' AND bytes_saved>0 THEN 1 ELSE 0 END),0) AS optimized_archives,COALESCE(SUM(image_count),0) AS pages,(SELECT COALESCE(SUM(f.file_count),0) FROM archive_formats f JOIN archives inventoried ON inventoried.id=f.archive_id WHERE inventoried.status!='missing' AND f.format='JPEG XL') AS converted_pages,COALESCE(SUM(CASE WHEN status='processed' THEN bytes_saved ELSE 0 END),0) AS net_bytes,COALESCE(SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END),0) AS failed_archives FROM archives WHERE status!='missing'", [])?.pop().unwrap_or_default();
    let attention = query_json(&connection, "SELECT COUNT(DISTINCT archive_id) AS archives,COUNT(*) AS issues FROM issues WHERE status IN ('open','queued')", [])?.pop().unwrap_or_default();
    let mut active = query_json(
        &connection,
        "SELECT * FROM jobs WHERE status IN ('queued','running','stopping') ORDER BY queued_at,id LIMIT 8",
        [],
    )?;
    let mut recent = query_json(
        &connection,
        "SELECT * FROM jobs ORDER BY queued_at DESC,id DESC LIMIT 8",
        [],
    )?;
    active
        .iter_mut()
        .chain(recent.iter_mut())
        .for_each(|value| {
            decode_json_field(value, "policy_json", "policy");
            decode_json_field(value, "action_json", "action");
            if let Some(object) = value.as_object_mut() {
                let completed = ["processed", "skipped", "failed", "deleted"]
                    .iter()
                    .filter_map(|key| object.get(*key).and_then(|value| value.as_i64()))
                    .sum::<i64>();
                let total = object
                    .get("archives_found")
                    .and_then(|value| value.as_i64())
                    .unwrap_or(0);
                let net = object
                    .get("net_bytes_saved")
                    .and_then(|value| value.as_i64())
                    .unwrap_or(0);
                object.insert("completed".into(), completed.into());
                object.insert(
                    "percent".into(),
                    if total > 0 {
                        (completed as f64 * 100.0 / total as f64).into()
                    } else {
                        0.into()
                    },
                );
                object.insert("net_gb".into(), (net as f64 / 1_073_741_824.0).into());
            }
        });
    if let Some(object) = totals.as_object_mut() {
        let net = object
            .get("net_bytes")
            .and_then(|value| value.as_i64())
            .unwrap_or(0);
        let pages = object.get("converted_pages").cloned().unwrap_or_default();
        let optimized = object
            .get("optimized_archives")
            .cloned()
            .unwrap_or_default();
        object.insert("net_gb".into(), (net as f64 / 1_073_741_824.0).into());
        object.insert("page_count".into(), pages);
        object.insert("optimized_count".into(), optimized);
    }
    let review =
        serde_json::json!({"total":attention.get("archives").cloned().unwrap_or_default()});
    Ok(
        serde_json::json!({"totals":totals,"attention":attention,"review":review,"active_jobs":active,"recent_jobs":recent}),
    )
}

async fn homepage(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let database = state.database.clone();
    let progress = state.progress.clone();
    let result = tokio::task::spawn_blocking(move || homepage_value(&database, &progress)).await;
    match result {
        Ok(Ok(value)) => (StatusCode::OK, Json(value)),
        Ok(Err(error)) => homepage_degraded(error.to_string()),
        Err(error) => homepage_degraded(error.to_string()),
    }
}

fn homepage_value(
    database: &crate::db::Database,
    progress: &ProgressHub,
) -> anyhow::Result<serde_json::Value> {
    let connection = database.connect()?;
    let totals = query_json(&connection, "SELECT COALESCE(SUM(CASE WHEN status='processed' THEN bytes_saved ELSE 0 END),0) AS net_bytes,(SELECT COUNT(DISTINCT archive_id) FROM issues WHERE status IN ('open','queued')) AS attention FROM archives", [])?.pop().unwrap_or_default();
    let active = query_json(&connection, "SELECT id,status,archives_found,processed,skipped,failed,deleted,queued_at,started_at FROM jobs WHERE status IN ('queued','running','stopping') ORDER BY CASE status WHEN 'running' THEN 0 WHEN 'stopping' THEN 1 ELSE 2 END,queued_at LIMIT 1", [])?.pop();
    let job_id = active
        .as_ref()
        .and_then(|job| job.get("id"))
        .and_then(|value| value.as_str());
    let latest = job_id.and_then(|id| progress.latest(id));
    let total = latest
        .as_ref()
        .and_then(|event| event.data.get("total"))
        .and_then(|value| value.as_i64())
        .or_else(|| {
            active
                .as_ref()
                .and_then(|job| job.get("archives_found"))
                .and_then(|value| value.as_i64())
        })
        .unwrap_or(0);
    let completed = latest
        .as_ref()
        .and_then(|event| event.data.get("completed"))
        .and_then(|value| value.as_i64())
        .or_else(|| {
            active.as_ref().map(|job| {
                ["processed", "skipped", "failed", "deleted"]
                    .iter()
                    .filter_map(|key| job.get(*key).and_then(|value| value.as_i64()))
                    .sum::<i64>()
            })
        })
        .unwrap_or(0);
    let run_status = active
        .as_ref()
        .and_then(|job| job.get("status"))
        .and_then(|value| value.as_str())
        .unwrap_or("idle");
    let current_archive = latest
        .as_ref()
        .and_then(|event| event.data.get("archive"))
        .and_then(|value| value.as_str())
        .and_then(|path| path.rsplit('/').next());
    Ok(serde_json::json!({
        "schema_version":1,"available":true,"version":VERSION,
        "status":if run_status=="idle"{"Idle"}else if run_status=="queued"{"Queued"}else{"Running"},
        "run_status":run_status,"run_id":job_id,
        "net_savings_bytes":totals.get("net_bytes").and_then(|value|value.as_i64()).unwrap_or(0),
        "attention_count":totals.get("attention").and_then(|value|value.as_i64()).unwrap_or(0),
        "completed_archives":completed,"total_archives":total,
        "failed_archives":active.as_ref().and_then(|job|job.get("failed")).and_then(|value|value.as_i64()).unwrap_or(0),
        "progress_percent":latest.as_ref().and_then(|event|event.data.get("progress_percent")).and_then(|value|value.as_f64()).unwrap_or_else(||if total>0{completed as f64/total as f64*100.0}else{0.0}),
        "current_archive":current_archive,
        "eta_seconds":latest.as_ref().and_then(|event|event.data.get("eta_seconds")).cloned().unwrap_or(serde_json::Value::Null),
        "rate_per_minute":latest.as_ref().and_then(|event|event.data.get("rate_per_minute")).and_then(|value|value.as_f64()).unwrap_or(0.0),
        "updated_at":latest.map(|event|event.at).or_else(||active.as_ref().and_then(|job|job.get("started_at").or_else(||job.get("queued_at"))).and_then(|value|value.as_str()).map(str::to_owned)),
    }))
}

fn homepage_degraded(error: String) -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::OK,
        Json(
            serde_json::json!({"schema_version":1,"available":false,"version":VERSION,"status":"Degraded","run_status":"unavailable","net_savings_bytes":0,"attention_count":0,"completed_archives":0,"total_archives":0,"failed_archives":0,"progress_percent":0,"current_archive":null,"eta_seconds":null,"rate_per_minute":0,"error":error}),
        ),
    )
}
