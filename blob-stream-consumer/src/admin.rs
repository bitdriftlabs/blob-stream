use crate::ConsumerDiagnostics;
use crate::diagnostics::ConsumerArmFreshStartResult;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use blob_stream_types::VirtualPartitionId;
use serde::Deserialize;

#[derive(Deserialize)]
struct ArmFreshStartRequest {
  confirm_loss: bool,
  #[serde(default)]
  dry_run: bool,
  virtual_partition_ids: Vec<VirtualPartitionId>,
}

#[derive(serde::Serialize)]
struct ArmFreshStartResponse {
  results: Vec<ConsumerArmFreshStartResult>,
}

pub fn router(diagnostics: ConsumerDiagnostics) -> Router {
  Router::new()
    .route(
      "/state",
      get({
        let diagnostics = diagnostics.clone();
        move || {
          let diagnostics = diagnostics.clone();
          async move { Json(diagnostics.state_response().await) }
        }
      }),
    )
    .route(
      "/partitions/arm-next-window-fresh-start",
      post(move |Json(request): Json<ArmFreshStartRequest>| {
        let diagnostics = diagnostics.clone();
        async move {
          if !request.dry_run && !request.confirm_loss {
            return (
              StatusCode::BAD_REQUEST,
              Json(serde_json::json!({
                "error": "confirm_loss must be true when dry_run is false",
              })),
            );
          }
          match diagnostics
            .arm_next_window_fresh_start(request.virtual_partition_ids, request.dry_run)
            .await
          {
            Ok(results) => (
              StatusCode::OK,
              Json(serde_json::json!(ArmFreshStartResponse { results })),
            ),
            Err(error) => (
              StatusCode::INTERNAL_SERVER_ERROR,
              Json(serde_json::json!({ "error": format!("{error:#}") })),
            ),
          }
        }
      }),
    )
}
