use super::{ApiError, AppState, decode_json_field, query_json};
use crate::{
    domain::{Policy, Scope, Strategy},
    schedule::Cadence,
};
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::{get, put},
};
use rusqlite::{OptionalExtension, params};
use serde::Deserialize;
use serde_json::json;
use std::{path::PathBuf, sync::Arc};
use uuid::Uuid;

pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/libraries", get(libraries).post(create_library))
        .route(
            "/libraries/{id}",
            put(update_library).delete(delete_library),
        )
        .route("/policies", get(policies).post(create_policy))
        .route("/policies/{id}", put(update_policy).delete(delete_policy))
        .route("/profiles", get(policies).post(create_policy))
        .route("/profiles/{id}", put(update_policy).delete(delete_policy))
        .route("/schedules", get(schedules).post(create_schedule))
        .route(
            "/schedules/{id}",
            put(update_schedule).delete(delete_schedule),
        )
}

#[derive(Debug, Deserialize)]
struct LibraryInput {
    name: String,
    root_path: PathBuf,
    #[serde(default = "enabled")]
    enabled: bool,
}

async fn libraries(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    Ok(Json(json!({"rows":state.database.libraries()?})))
}

async fn create_library(
    State(state): State<Arc<AppState>>,
    Json(input): Json<LibraryInput>,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    validate_library(&input)?;
    let id = Uuid::new_v4().to_string();
    let now = crate::db::now();
    state.database.connect()?.execute(
        "INSERT INTO libraries(id,name,root_path,enabled,created_at,updated_at) VALUES (?,?,?,?,?,?)",
        params![id,input.name,input.root_path.to_string_lossy(),input.enabled as i64,now,now],
    )?;
    Ok((StatusCode::CREATED, Json(json!({"id":id}))))
}

async fn update_library(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(input): Json<LibraryInput>,
) -> Result<Json<serde_json::Value>, ApiError> {
    validate_library(&input)?;
    let changed = state.database.connect()?.execute(
        "UPDATE libraries SET name=?,root_path=?,enabled=?,updated_at=? WHERE id=?",
        params![
            input.name,
            input.root_path.to_string_lossy(),
            input.enabled as i64,
            crate::db::now(),
            id
        ],
    )?;
    if changed == 0 {
        return Err(ApiError::not_found(anyhow::anyhow!("library not found")));
    }
    Ok(Json(json!({"id":id})))
}

async fn delete_library(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let connection = state.database.connect()?;
    let used: i64 = connection.query_row("SELECT (SELECT COUNT(*) FROM jobs WHERE library_id=?)+(SELECT COUNT(*) FROM archives WHERE library_id=?)", params![id,id], |row| row.get(0))?;
    if used > 0 {
        return Err(ApiError::conflict(anyhow::anyhow!(
            "library has archive or job history"
        )));
    }
    if connection.execute("DELETE FROM libraries WHERE id=?", [&id])? == 0 {
        return Err(ApiError::not_found(anyhow::anyhow!("library not found")));
    }
    Ok(StatusCode::NO_CONTENT)
}

fn validate_library(input: &LibraryInput) -> Result<(), ApiError> {
    if input.name.trim().is_empty() {
        return Err(ApiError::bad_request(anyhow::anyhow!(
            "library name is required"
        )));
    }
    if !input.root_path.is_absolute() || !input.root_path.is_dir() {
        return Err(ApiError::bad_request(anyhow::anyhow!(
            "library root must be an existing absolute directory"
        )));
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
struct PolicyInput {
    name: String,
    #[serde(default)]
    is_default: bool,
    policy: Option<Policy>,
    options: Option<serde_json::Value>,
}

impl PolicyInput {
    fn resolved(&self) -> Result<Policy, ApiError> {
        if let Some(policy) = &self.policy {
            return Ok(policy.clone());
        }
        let value = self.options.as_ref().ok_or_else(|| {
            ApiError::bad_request(anyhow::anyhow!("policy or options is required"))
        })?;
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
        Ok(Policy {
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
        })
    }
}

async fn policies(State(state): State<Arc<AppState>>) -> Result<Json<serde_json::Value>, ApiError> {
    let connection = state.database.connect()?;
    let mut rows = query_json(
        &connection,
        "SELECT * FROM policies ORDER BY is_default DESC,name,id",
        [],
    )?;
    rows.iter_mut().for_each(|row| {
        decode_json_field(row, "policy_json", "policy");
        if let Some(policy) = row.get("policy").cloned()
            && let Some(object) = row.as_object_mut()
        {
            object.insert("options".into(), policy_to_options(&policy));
        }
    });
    Ok(Json(json!({"rows":rows})))
}

async fn create_policy(
    State(state): State<Arc<AppState>>,
    Json(input): Json<PolicyInput>,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    validate_name(&input.name)?;
    let id = Uuid::new_v4().to_string();
    let now = crate::db::now();
    let mut connection = state.database.connect()?;
    let transaction = connection.transaction()?;
    if input.is_default {
        transaction.execute("UPDATE policies SET is_default=0", [])?;
    }
    transaction.execute("INSERT INTO policies(id,name,is_default,policy_json,created_at,updated_at) VALUES (?,?,?,?,?,?)",params![id,input.name,input.is_default as i64,serde_json::to_string(&input.resolved()?.normalize(state.config.cpu_limit))?,now,now])?;
    transaction.commit()?;
    Ok((StatusCode::CREATED, Json(json!({"id":id}))))
}

async fn update_policy(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(input): Json<PolicyInput>,
) -> Result<Json<serde_json::Value>, ApiError> {
    validate_name(&input.name)?;
    let mut connection = state.database.connect()?;
    let transaction = connection.transaction()?;
    if input.is_default {
        transaction.execute("UPDATE policies SET is_default=0 WHERE id!=?", [&id])?;
    }
    let changed = transaction.execute(
        "UPDATE policies SET name=?,is_default=?,policy_json=?,updated_at=? WHERE id=?",
        params![
            input.name,
            input.is_default as i64,
            serde_json::to_string(&input.resolved()?.normalize(state.config.cpu_limit))?,
            crate::db::now(),
            id
        ],
    )?;
    if changed == 0 {
        return Err(ApiError::not_found(anyhow::anyhow!("policy not found")));
    }
    transaction.commit()?;
    Ok(Json(json!({"id":id})))
}

async fn delete_policy(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let mut connection = state.database.connect()?;
    let transaction = connection.transaction()?;
    let replacement: Option<String> = transaction
        .query_row(
            "SELECT id FROM policies WHERE id!=? ORDER BY is_default DESC,created_at LIMIT 1",
            [&id],
            |row| row.get(0),
        )
        .optional()?;
    let replacement = replacement
        .ok_or_else(|| ApiError::conflict(anyhow::anyhow!("the only policy cannot be deleted")))?;
    transaction.execute(
        "UPDATE schedules SET policy_id=?,updated_at=? WHERE policy_id=?",
        params![replacement, crate::db::now(), id],
    )?;
    if transaction.execute("DELETE FROM policies WHERE id=?", [&id])? == 0 {
        return Err(ApiError::not_found(anyhow::anyhow!("policy not found")));
    }
    transaction.execute("UPDATE policies SET is_default=1 WHERE id=? AND NOT EXISTS(SELECT 1 FROM policies WHERE is_default=1)",[replacement])?;
    transaction.commit()?;
    Ok(StatusCode::NO_CONTENT)
}

fn validate_name(name: &str) -> Result<(), ApiError> {
    if name.trim().is_empty() || name.chars().count() > 80 {
        return Err(ApiError::bad_request(anyhow::anyhow!(
            "name must contain 1 to 80 characters"
        )));
    }
    Ok(())
}

fn policy_to_options(policy: &serde_json::Value) -> serde_json::Value {
    let strategy = policy
        .get("strategy")
        .and_then(|value| value.as_str())
        .unwrap_or("lossless");
    let scope = policy
        .get("scope")
        .and_then(|value| value.as_str())
        .unwrap_or("unprocessed");
    json!({"effort":policy.get("effort"),"threads":policy.get("threads"),"backup":policy.get("backup"),"distance":if matches!(strategy,"smart"|"lossy"){policy.get("distance").cloned().unwrap_or(0.5.into())}else{serde_json::Value::Null},"smart_mb":policy.get("smart_mb"),"smart_mp":policy.get("smart_mp"),"smart_distance":strategy=="smart","no_convert":matches!(strategy,"recheck"|"audit"),"integrity_audit":strategy=="audit","recheck_all":strategy=="audit"||scope=="library","failed_only":strategy!="audit"&&scope=="failed"})
}

#[derive(Debug, Deserialize)]
struct ScheduleInput {
    name: String,
    #[serde(default = "enabled")]
    enabled: bool,
    library_id: String,
    policy_id: Option<String>,
    profile_id: Option<String>,
    #[serde(flatten)]
    cadence: Cadence,
}

impl ScheduleInput {
    fn policy_id(&self) -> Result<&str, ApiError> {
        self.policy_id
            .as_deref()
            .or(self.profile_id.as_deref())
            .ok_or_else(|| {
                ApiError::bad_request(anyhow::anyhow!("policy_id or profile_id is required"))
            })
    }
}

async fn schedules(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let connection = state.database.connect()?;
    let mut rows = query_json(
        &connection,
        "SELECT s.*,l.name AS library_name,p.name AS policy_name FROM schedules s JOIN libraries l ON l.id=s.library_id JOIN policies p ON p.id=s.policy_id ORDER BY s.created_at,id",
        [],
    )?;
    rows.iter_mut().for_each(|row| {
        decode_json_field(row, "cadence_json", "cadence_options");
        let cadence = row.get("cadence_options").cloned().unwrap_or_default();
        if let Some(object) = row.as_object_mut() {
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
            let name = object
                .get("cadence")
                .and_then(|value| value.as_str())
                .unwrap_or("daily");
            let description = match name {
                "interval" => format!(
                    "Every {} {}",
                    cadence.get("value").and_then(|v| v.as_i64()).unwrap_or(1),
                    cadence
                        .get("unit")
                        .and_then(|v| v.as_str())
                        .unwrap_or("hours")
                ),
                _ => format!(
                    "{} · {:02}:{:02}",
                    name,
                    cadence.get("hour").and_then(|v| v.as_i64()).unwrap_or(0),
                    cadence.get("minute").and_then(|v| v.as_i64()).unwrap_or(0)
                ),
            };
            object.insert("description".into(), description.into());
        }
    });
    Ok(Json(json!({"rows":rows})))
}

async fn create_schedule(
    State(state): State<Arc<AppState>>,
    Json(input): Json<ScheduleInput>,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    validate_schedule(&input)?;
    let id = Uuid::new_v4().to_string();
    save_schedule(&state, &id, &input, true)?;
    Ok((StatusCode::CREATED, Json(json!({"id":id}))))
}

async fn update_schedule(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(input): Json<ScheduleInput>,
) -> Result<Json<serde_json::Value>, ApiError> {
    validate_schedule(&input)?;
    save_schedule(&state, &id, &input, false)?;
    Ok(Json(json!({"id":id})))
}

fn save_schedule(
    state: &AppState,
    id: &str,
    input: &ScheduleInput,
    create: bool,
) -> Result<(), ApiError> {
    let connection = state.database.connect()?;
    let now = crate::db::now();
    let next = input
        .enabled
        .then(|| input.cadence.next(&state.config.timezone))
        .transpose()
        .map_err(ApiError::bad_request)?;
    if create{
        connection.execute("INSERT INTO schedules(id,name,enabled,cadence,cadence_json,library_id,policy_id,next_run_at,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)",params![id,input.name,input.enabled as i64,input.cadence.name(),serde_json::to_string(&input.cadence)?,input.library_id,input.policy_id()?,next,now,now])?;
    }else if connection.execute("UPDATE schedules SET name=?,enabled=?,cadence=?,cadence_json=?,library_id=?,policy_id=?,next_run_at=?,updated_at=? WHERE id=?",params![input.name,input.enabled as i64,input.cadence.name(),serde_json::to_string(&input.cadence)?,input.library_id,input.policy_id()?,next,now,id])?==0{
        return Err(ApiError::not_found(anyhow::anyhow!("schedule not found")));
    }
    Ok(())
}

async fn delete_schedule(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    if state
        .database
        .connect()?
        .execute("DELETE FROM schedules WHERE id=?", [id])?
        == 0
    {
        return Err(ApiError::not_found(anyhow::anyhow!("schedule not found")));
    }
    Ok(StatusCode::NO_CONTENT)
}

fn validate_schedule(input: &ScheduleInput) -> Result<(), ApiError> {
    validate_name(&input.name)?;
    input.cadence.validate().map_err(ApiError::bad_request)?;
    Ok(())
}

const fn enabled() -> bool {
    true
}
