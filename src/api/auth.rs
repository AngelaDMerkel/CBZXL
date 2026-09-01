use super::AppState;
use axum::{
    Json,
    extract::{Request, State},
    http::{StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use std::sync::Arc;
use subtle::ConstantTimeEq;

pub async fn authorize(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Response {
    let Some(expected) = &state.config.api_token else {
        return next.run(request).await;
    };
    let bearer = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    let supplied = bearer;
    if supplied.is_some_and(|value| {
        value.len() == expected.len() && bool::from(value.as_bytes().ct_eq(expected.as_bytes()))
    }) {
        return next.run(request).await;
    }
    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, "Bearer")],
        Json(serde_json::json!({"error":"unauthorized","message":"A valid API token is required"})),
    )
        .into_response()
}
