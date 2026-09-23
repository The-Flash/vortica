use std::{convert::Infallible, sync::Arc, time::Duration};

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{
        Sse,
        sse::{Event, KeepAlive},
    },
    routing::{get, post},
};
use serde::Serialize;
use tokio_stream::{Stream, StreamExt, wrappers::BroadcastStream};
use uuid::Uuid;

#[derive(Copy, Clone, Debug, Serialize)]
enum VortexState {
    Forming,
    Spinning,
    Stalling,
    Dissipated,
    Collapsed,
}

#[derive(Copy, Clone, Debug)]
struct Vortex {
    id: Uuid,
    state: VortexState,
}

#[derive(Clone, Debug, Serialize)]
enum VortexEvent {
    Event { id: Uuid, state: VortexState },
}

struct AppState {
    tx_wormhole: tokio::sync::mpsc::Sender<Vortex>,
    tx_events: tokio::sync::broadcast::Sender<VortexEvent>,
}

async fn vortex_create(
    State(state): State<Arc<AppState>>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<serde_json::Value>)> {
    let vortex_id = Uuid::new_v4();
    let new_vortex = Vortex {
        id: vortex_id,
        state: VortexState::Forming,
    };
    state.tx_wormhole.send(new_vortex).await.map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "message": "Could not send message",
            })),
        )
    })?;

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "events_url": format!("/vortex/{}/stream", vortex_id),
        })),
    ))
}

async fn vortex_stream(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = state.tx_events.subscribe();
    let stream = BroadcastStream::new(rx).filter_map(move |result| {
        let event = result.ok()?;

        match event {
            VortexEvent::Event {
                id: vortex_id,
                state: _,
            } => {
                if vortex_id == id {
                    return Some(Ok(Event::default()
                        .event("vortex")
                        .json_data(event)
                        .ok()?));
                } else {
                    return None;
                }
            }
        }
    });

    Sse::new(stream).keep_alive(KeepAlive::default())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (tx_wormhole, mut rx_wormhole) = tokio::sync::mpsc::channel::<Vortex>(100);
    let (tx_events, _rx_events) = tokio::sync::broadcast::channel::<VortexEvent>(100);
    let state = Arc::new(AppState {
        tx_wormhole,
        tx_events,
    });

    let worker_state = state.clone();
    // Spawn worker
    tokio::spawn(async move {
        while let Some(vortex) = rx_wormhole.recv().await {
            let vortex_states = vec![
                VortexState::Spinning,
                VortexState::Stalling,
                VortexState::Dissipated,
                VortexState::Collapsed,
            ];
            for s in vortex_states {
                tokio::time::sleep(Duration::from_secs(10)).await;
                let _ = worker_state.tx_events.send(VortexEvent::Event {
                    id: vortex.id,
                    state: s,
                });
            }
        }
    });

    let app = Router::new()
        .route("/vortex", post(vortex_create))
        .route("/vortex/{id}/stream", get(vortex_stream))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    axum::serve(listener, app).await?;

    Ok(())
}
