mod auth;
mod jobs;
mod library;
mod settings;
mod system;

use crate::{config::Config, db::Database, progress::ProgressHub};
use anyhow::{Context, Result};
use axum::{
    Json, Router,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use rusqlite::{Connection, Params, types::ValueRef};
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::RwLock;
use tower_http::{
    compression::CompressionLayer, services::ServeDir, set_header::SetResponseHeaderLayer,
    trace::TraceLayer,
};

#[derive(Clone)]
pub(crate) struct AppState {
    pub config: Config,
    pub database: Database,
    pub progress: ProgressHub,
    pub runtime_error: Arc<RwLock<Option<String>>>,
    pub database_error: Arc<RwLock<Option<String>>>,
}

#[derive(Debug)]
pub(crate) struct ApiError {
    status: StatusCode,
    error: anyhow::Error,
}

pub(crate) fn query_json<P: Params>(
    connection: &Connection,
    sql: &str,
    parameters: P,
) -> anyhow::Result<Vec<serde_json::Value>> {
    let mut statement = connection.prepare(sql)?;
    let names: Vec<_> = statement
        .column_names()
        .into_iter()
        .map(str::to_owned)
        .collect();
    let mut rows = statement.query(parameters)?;
    let mut values = Vec::new();
    while let Some(row) = rows.next()? {
        let mut object = serde_json::Map::new();
        for (index, name) in names.iter().enumerate() {
            let value = match row.get_ref(index)? {
                ValueRef::Null => serde_json::Value::Null,
                ValueRef::Integer(value) => value.into(),
                ValueRef::Real(value) => value.into(),
                ValueRef::Text(value) => String::from_utf8_lossy(value).into_owned().into(),
                ValueRef::Blob(value) => hex::encode(value).into(),
            };
            object.insert(name.clone(), value);
        }
        values.push(object.into());
    }
    Ok(values)
}

pub(crate) fn decode_json_field(value: &mut serde_json::Value, source: &str, target: &str) {
    let Some(object) = value.as_object_mut() else {
        return;
    };
    let decoded = object
        .remove(source)
        .and_then(|value| {
            value
                .as_str()
                .and_then(|value| serde_json::from_str(value).ok())
        })
        .unwrap_or_default();
    object.insert(target.into(), decoded);
}

impl ApiError {
    pub fn bad_request(error: impl Into<anyhow::Error>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            error: error.into(),
        }
    }

    pub fn not_found(error: impl Into<anyhow::Error>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            error: error.into(),
        }
    }

    pub fn conflict(error: impl Into<anyhow::Error>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            error: error.into(),
        }
    }

    pub fn unavailable(error: impl Into<anyhow::Error>) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            error: error.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(serde_json::json!({
                "error": self.status.canonical_reason().unwrap_or("request_error").to_lowercase().replace(' ', "_"),
                "message": self.error.to_string()
            })),
        )
            .into_response()
    }
}

impl<E> From<E> for ApiError
where
    E: Into<anyhow::Error>,
{
    fn from(value: E) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            error: value.into(),
        }
    }
}

pub async fn serve(
    config: Config,
    database: Database,
    embedded_worker: bool,
    initial_database_error: Option<String>,
) -> Result<()> {
    let address: SocketAddr = config.listen.parse().context("invalid LISTEN_ADDR")?;
    if config.api_token.is_none() && !address.ip().is_loopback() {
        tracing::warn!(%address,"API authentication is disabled; destructive routes are available to every client that can reach CBZXL");
    }
    let frontend = config.frontend.clone();
    let progress = ProgressHub::at(config.runtime.clone());
    let runtime_error = Arc::new(RwLock::new(Some(
        "Conversion runtime check in progress".into(),
    )));
    let database_error = Arc::new(RwLock::new(initial_database_error));
    let state = Arc::new(AppState {
        config: config.clone(),
        database: database.clone(),
        progress: progress.clone(),
        runtime_error: runtime_error.clone(),
        database_error: database_error.clone(),
    });
    if embedded_worker {
        let worker_status = runtime_error.clone();
        let _worker_thread = std::thread::Builder::new()
            .name("cbzxl-worker-supervisor".into())
            .spawn(move || {
                match tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .thread_name("cbzxl-worker")
                    .build()
                {
                    Ok(runtime) => {
                        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            runtime.block_on(supervise_embedded_worker(
                                config,
                                database,
                                progress,
                                runtime_error,
                                database_error,
                            ))
                        }));
                        if result.is_err() {
                            *worker_status.blocking_write() =
                                Some("Worker stopped after an internal panic".into());
                            tracing::error!("embedded worker panicked and stopped");
                        }
                    }
                    Err(error) => {
                        let message = format!("Worker runtime unavailable: {error}");
                        *worker_status.blocking_write() = Some(message.clone());
                        tracing::error!(%error, "could not create worker runtime");
                    }
                }
            })
            .context("could not start worker supervisor thread")?;
    } else {
        let database_status = state.database_error.clone();
        let database = state.database.clone();
        tokio::spawn(async move {
            loop {
                match database.migrate() {
                    Ok(()) => {
                        *database_status.write().await = None;
                        break;
                    }
                    Err(error) => *database_status.write().await = Some(error.to_string()),
                }
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
            }
        });
        let status = state.runtime_error.clone();
        tokio::spawn(async move {
            use crate::engine::MediaBackend;
            let error = tokio::task::spawn_blocking(|| {
                crate::engine::CommandMediaBackend::default().doctor()
            })
            .await
            .ok()
            .and_then(Result::err)
            .map(|error| error.to_string());
            *status.write().await = error;
        });
    }

    let app = router(state, frontend);
    let listener = tokio::net::TcpListener::bind(address).await?;
    tracing::info!(%address, embedded_worker, "CBZXL ready");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn supervise_embedded_worker(
    config: Config,
    database: Database,
    progress: ProgressHub,
    runtime_error: Arc<RwLock<Option<String>>>,
    database_error: Arc<RwLock<Option<String>>>,
) {
    loop {
        if let Err(error) = database.migrate() {
            *database_error.write().await = Some(error.to_string());
            tracing::error!(%error, "database unavailable; worker waiting");
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
            continue;
        }
        *database_error.write().await = None;
        if let Err(error) = crate::worker::run_with_status(
            config.clone(),
            database.clone(),
            progress.clone(),
            runtime_error.clone(),
        )
        .await
        {
            tracing::error!(%error, "embedded worker stopped; restarting");
            *runtime_error.write().await = Some(format!("Worker stopped: {error}"));
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
    }
}

fn router(state: Arc<AppState>, frontend: std::path::PathBuf) -> Router {
    let protected = Router::new()
        .merge(jobs::routes())
        .merge(library::routes())
        .merge(settings::routes())
        .merge(system::protected_routes())
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth::authorize,
        ));
    let legacy = protected.clone();
    Router::new()
        .merge(system::public_routes())
        .nest("/api/v2", protected)
        .nest("/api", legacy)
        .fallback_service(ServeDir::new(frontend).append_index_html_on_directories(true))
        .layer(SetResponseHeaderLayer::if_not_present(
            header::X_CONTENT_TYPE_OPTIONS,
            axum::http::HeaderValue::from_static("nosniff"),
        ))
        .layer(SetResponseHeaderLayer::if_not_present(
            header::X_FRAME_OPTIONS,
            axum::http::HeaderValue::from_static("DENY"),
        ))
        .layer(SetResponseHeaderLayer::if_not_present(
            header::REFERRER_POLICY,
            axum::http::HeaderValue::from_static("no-referrer"),
        ))
        .layer(SetResponseHeaderLayer::if_not_present(
            header::CONTENT_SECURITY_POLICY,
            axum::http::HeaderValue::from_static(
                "default-src 'self'; img-src 'self' blob: data:; style-src 'self' 'unsafe-inline'; script-src 'self'; connect-src 'self'; object-src 'none'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'",
            ),
        ))
        .layer(SetResponseHeaderLayer::if_not_present(
            axum::http::HeaderName::from_static("permissions-policy"),
            axum::http::HeaderValue::from_static("camera=(), microphone=(), geolocation=()"),
        ))
        .layer(CompressionLayer::new())
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request, StatusCode, header},
    };
    use tower::ServiceExt;

    fn config(root: &std::path::Path) -> Config {
        Config {
            database: root.join("cbzxl.sqlite3"),
            library: root.into(),
            runtime: root.join("runtime"),
            scratch: root.join("scratch"),
            frontend: root.join("frontend"),
            listen: "127.0.0.1:0".into(),
            api_token: Some("correct-token".into()),
            timezone: "UTC".into(),
            cpu_limit: 2,
            archive_limit: 1024 * 1024,
            stable_for: std::time::Duration::ZERO,
            poll_every: std::time::Duration::from_secs(1),
            encoder_timeout: std::time::Duration::ZERO,
            auxiliary_timeout: std::time::Duration::from_secs(30),
            fingerprint_audit_days: 30,
            ntfy_url: None,
            ntfy_token: None,
            ntfy_timeout: std::time::Duration::from_secs(10),
            page_outcome_retention_days: 180,
            durable_checkpoint: std::time::Duration::from_secs(300),
            durable_checkpoint_archives: 10_000,
            database_maintenance: std::time::Duration::from_secs(604_800),
            statistics_cache: std::time::Duration::from_secs(30),
            worker_stall: std::time::Duration::from_secs(900),
            auto_retry_attempts: 1,
        }
    }

    #[tokio::test]
    async fn api_token_protects_application_routes_only() {
        let root = tempfile::tempdir().unwrap();
        let config = config(root.path());
        let database = Database::open(config.database.clone()).unwrap();
        database.migrate().unwrap();
        let state = Arc::new(AppState {
            config: config.clone(),
            database,
            progress: ProgressHub::default(),
            runtime_error: Arc::new(RwLock::new(None)),
            database_error: Arc::new(RwLock::new(None)),
        });
        let app = router(state, config.frontend);

        let health = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(health.status(), StatusCode::OK);
        let denied = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v2/bootstrap")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
        let allowed = app
            .oneshot(
                Request::builder()
                    .uri("/api/v2/bootstrap")
                    .header(header::AUTHORIZATION, "Bearer correct-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(allowed.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn authenticated_job_creation_is_durable() {
        let root = tempfile::tempdir().unwrap();
        let config = config(root.path());
        let database = Database::open(config.database.clone()).unwrap();
        database.migrate().unwrap();
        let state = Arc::new(AppState {
            config: config.clone(),
            database: database.clone(),
            progress: ProgressHub::default(),
            runtime_error: Arc::new(RwLock::new(None)),
            database_error: Arc::new(RwLock::new(None)),
        });
        let app = router(state, config.frontend);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v2/jobs")
                    .header(header::AUTHORIZATION, "Bearer correct-token")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let count: i64 = database
            .connect()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM jobs", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn database_failure_keeps_health_and_bootstrap_online() {
        let root = tempfile::tempdir().unwrap();
        let config = config(root.path());
        let database = Database::open(config.database.clone()).unwrap();
        let state = Arc::new(AppState {
            config: config.clone(),
            database,
            progress: ProgressHub::default(),
            runtime_error: Arc::new(RwLock::new(None)),
            database_error: Arc::new(RwLock::new(Some("storage unavailable".into()))),
        });
        let app = router(state, config.frontend);
        let health = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(health.status(), StatusCode::OK);
        let ready = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/ready")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ready.status(), StatusCode::SERVICE_UNAVAILABLE);
        let bootstrap = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v2/bootstrap")
                    .header(header::AUTHORIZATION, "Bearer correct-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(bootstrap.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(bootstrap.into_body(), usize::MAX)
            .await
            .unwrap();
        assert!(String::from_utf8_lossy(&bytes).contains("storage unavailable"));
        let docs = app
            .oneshot(
                Request::builder()
                    .uri("/api/docs")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(docs.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn parity_routes_expose_history_reviews_and_legacy_paths() {
        let root = tempfile::tempdir().unwrap();
        let config = config(root.path());
        let database = Database::open(config.database.clone()).unwrap();
        database.migrate().unwrap();
        let job = database
            .enqueue(
                "library-default",
                Some("profile-balanced"),
                &crate::domain::Policy::default(),
                crate::domain::JobSource::Manual,
                None,
            )
            .unwrap();
        let connection = database.connect().unwrap();
        let now = crate::db::now();
        connection.execute("INSERT INTO archives(library_id,path,status,baseline_size,current_size,created_at,updated_at) VALUES ('library-default','Book.cbz','processed',100,80,?,?)",rusqlite::params![now,now]).unwrap();
        let archive_id = connection.last_insert_rowid();
        connection.execute("INSERT INTO job_archive_outcomes(job_id,archive_id,path,outcome,source_size,final_size,created_at) VALUES (?,?,'Book.cbz','optimized',100,80,?)",rusqlite::params![job,archive_id,now]).unwrap();
        let outcome_id = connection.last_insert_rowid();
        connection.execute("INSERT INTO page_outcomes(job_outcome_id,entry_path,outcome,attempts_json,created_at) VALUES (?,'001.jpg','converted','[]',?)",rusqlite::params![outcome_id,now]).unwrap();
        let state = Arc::new(AppState {
            config: config.clone(),
            database,
            progress: ProgressHub::default(),
            runtime_error: Arc::new(RwLock::new(None)),
            database_error: Arc::new(RwLock::new(None)),
        });
        let app = router(state, config.frontend);
        let pages = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/jobs/{job}/page-decisions"))
                    .header(header::AUTHORIZATION, "Bearer correct-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(pages.status(), StatusCode::OK);
        let review = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/archives/{archive_id}/review"))
                    .header(header::AUTHORIZATION, "Bearer correct-token")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"action":"reviewed","note":"checked"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(review.status(), StatusCode::OK);
        let legacy = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/bootstrap")
                    .header(header::AUTHORIZATION, "Bearer correct-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(legacy.status(), StatusCode::OK);
        let homepage = app
            .oneshot(
                Request::builder()
                    .uri("/api/integrations/homepage")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(homepage.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn history_counts_are_complete_and_backup_remedies_are_contextual() {
        let root = tempfile::tempdir().unwrap();
        let config = config(root.path());
        let database = Database::open(config.database.clone()).unwrap();
        database.migrate().unwrap();
        let job = database
            .enqueue(
                "library-default",
                Some("profile-balanced"),
                &crate::domain::Policy::default(),
                crate::domain::JobSource::Manual,
                None,
            )
            .unwrap();
        let mut connection = database.connect().unwrap();
        let now = crate::db::now();
        connection.execute("INSERT INTO archives(library_id,path,status,baseline_size,current_size,created_at,updated_at) VALUES ('library-default','Book.cbz','processed',100,120,?,?)",rusqlite::params![now,now]).unwrap();
        let archive_id = connection.last_insert_rowid();
        let transaction = connection.transaction().unwrap();
        for index in 0..600 {
            transaction.execute(
                "INSERT INTO job_archive_outcomes(job_id,archive_id,path,outcome,source_size,final_size,created_at) VALUES (?,?,?,'retained',100,100,?)",
                rusqlite::params![job,archive_id,format!("Book-{index:03}.cbz"),now],
            ).unwrap();
        }
        transaction.execute("INSERT INTO issues(archive_id,job_id,kind,stage,message,status,created_at,updated_at) VALUES (?,?,'archive_grew','verifying','Larger than baseline','open',?,?)",rusqlite::params![archive_id,job,now,now]).unwrap();
        transaction.commit().unwrap();
        let state = Arc::new(AppState {
            config: config.clone(),
            database,
            progress: ProgressHub::default(),
            runtime_error: Arc::new(RwLock::new(None)),
            database_error: Arc::new(RwLock::new(None)),
        });
        let app = router(state, config.frontend);
        let history = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/v2/jobs/{job}/history?limit=5"))
                    .header(header::AUTHORIZATION, "Bearer correct-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(history.status(), StatusCode::OK);
        let body = axum::body::to_bytes(history.into_body(), usize::MAX)
            .await
            .unwrap();
        let history: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(history["outcome_counts"]["all"], 600);
        assert_eq!(history["history_entries"].as_array().unwrap().len(), 5);
        assert!(history["next_outcome"].is_number());

        let issues = app
            .oneshot(
                Request::builder()
                    .uri("/api/v2/issues?group=archive_grew:verifying")
                    .header(header::AUTHORIZATION, "Bearer correct-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(issues.into_body(), usize::MAX)
            .await
            .unwrap();
        let issues: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(issues["rows"][0]["backup_available"], false);
        assert_eq!(
            issues["rows"][0]["remedies"],
            serde_json::json!(["repack", "dismiss"])
        );
    }

    #[tokio::test]
    async fn statistics_describe_current_inventory_and_exclude_skip_timings() {
        let root = tempfile::tempdir().unwrap();
        let config = config(root.path());
        let database = Database::open(config.database.clone()).unwrap();
        database.migrate().unwrap();
        let job = database
            .enqueue(
                "library-default",
                Some("profile-balanced"),
                &crate::domain::Policy::default(),
                crate::domain::JobSource::Manual,
                None,
            )
            .unwrap();
        let connection = database.connect().unwrap();
        let now = crate::db::now();
        connection.execute("INSERT INTO archives(library_id,path,status,baseline_size,current_size,bytes_saved,percent_saved,image_count,retained_pages,created_at,updated_at) VALUES ('library-default','Book.cbz','processed',100,80,20,20,1,0,?,?)",rusqlite::params![now,now]).unwrap();
        let archive_id = connection.last_insert_rowid();
        connection.execute("INSERT INTO archive_formats(archive_id,format,file_count,compressed_bytes,uncompressed_bytes,updated_at) VALUES (?,'JPEG XL',1,80,80,?)",rusqlite::params![archive_id,now]).unwrap();
        connection.execute("INSERT INTO job_archive_outcomes(job_id,archive_id,path,outcome,source_size,final_size,duration_ms,created_at) VALUES (?,?,'Book.cbz','retained',80,80,10000,?)",rusqlite::params![job,archive_id,now]).unwrap();
        connection.execute("INSERT INTO job_archive_outcomes(job_id,archive_id,path,outcome,source_size,final_size,duration_ms,created_at) VALUES (?,?,'Skipped.cbz','already_current',80,80,1,?)",rusqlite::params![job,archive_id,now]).unwrap();
        let state = Arc::new(AppState {
            config: config.clone(),
            database,
            progress: ProgressHub::default(),
            runtime_error: Arc::new(RwLock::new(None)),
            database_error: Arc::new(RwLock::new(None)),
        });
        let response = router(state, config.frontend)
            .oneshot(
                Request::builder()
                    .uri("/api/v2/statistics")
                    .header(header::AUTHORIZATION, "Bearer correct-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["library"]["optimized_archives"], 1);
        assert_eq!(value["library"]["jxl_coverage_percent"], 100.0);
        assert_eq!(value["filetypes"]["inventoried_archives"], 1);
        assert_eq!(value["processing"]["median_seconds"], 10.0);
    }
}
