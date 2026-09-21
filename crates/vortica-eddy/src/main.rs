use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{get, post},
};
use uuid::Uuid;

#[derive(Copy, Clone)]
enum VortexState {
    Forming,
    Spinning,
    Stalling,
    Dissipated,
    Collapsed,
}

#[derive(Copy, Clone)]
struct Vortex {
    state: VortexState,
}

struct AppState {
    vortexes: RwLock<HashMap<Uuid, Vortex>>,
}

async fn vortex(
    State(state): State<Arc<AppState>>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<serde_json::Value>)> {
    let job_id = Uuid::new_v4();
    let mut vortexes = state.vortexes.write().map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "message": "Failed to acquire write lock on vortexes",
            })),
        )
    })?;
    let new_vortex = Vortex {
        state: VortexState::Forming,
    };
    vortexes.insert(job_id, new_vortex);
    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "events_url": format!("/vortex/{}/stream", job_id),
        })),
    ))
}

async fn vortex_stream() -> &'static str {
    "Vortex stream endpoint"
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let state = Arc::new(AppState {
        vortexes: RwLock::new(HashMap::new()),
    });

    let app = Router::new()
        .route("/vortex", post(vortex))
        .route("/vortex/{id}/stream", get(vortex_stream))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    axum::serve(listener, app).await?;

    Ok(())
}
