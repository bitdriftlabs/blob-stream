use crate::ConsumerDiagnostics;
use axum::routing::get;
use axum::{Json, Router};

pub fn router(diagnostics: ConsumerDiagnostics) -> Router {
  Router::new().route(
    "/state",
    get(move || {
      let diagnostics = diagnostics.clone();
      async move { Json(diagnostics.state_response().await) }
    }),
  )
}
