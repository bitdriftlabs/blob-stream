use crate::metrics::BrokerMetrics;
use crate::write::{WriteEngine, WriteError, WriteRequest};
use axum::Router;
use axum::extract::Query;
use axum::http::header::CONTENT_TYPE;
use axum::routing::{get, post};
use bd_grpc::Handler;
use bd_grpc::service::ServiceMethod;
use bd_log::{SwapLogger, warn_every};
use bd_server_stats::stats::Scope;
use blob_stream_proto::protos::blobstream::v1::broker::{
  ProduceBatchRequest,
  ProduceBatchResponse,
  ProduceStatus,
};
use blob_stream_types::Record;
use http::{Extensions, HeaderMap};
use log::trace;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use time::ext::NumericalDuration;

//
// BrokerGrpcMetrics
//

struct BrokerGrpcMetrics {
  requests_total: prometheus::IntCounter,
  records_total: prometheus::IntCounter,
  responses_ok_total: prometheus::IntCounter,
  responses_not_lease_holder_total: prometheus::IntCounter,
  responses_unknown_topic_total: prometheus::IntCounter,
  responses_overloaded_total: prometheus::IntCounter,
  request_latency_seconds: prometheus::Histogram,
}

impl BrokerGrpcMetrics {
  fn new(scope: &Scope) -> Self {
    let scope = scope.scope("grpc");
    Self {
      requests_total: scope.counter("requests_total"),
      records_total: scope.counter("records_total"),
      responses_ok_total: scope.counter("responses_ok_total"),
      responses_not_lease_holder_total: scope.counter("responses_not_lease_holder_total"),
      responses_unknown_topic_total: scope.counter("responses_unknown_topic_total"),
      responses_overloaded_total: scope.counter("responses_overloaded_total"),
      request_latency_seconds: scope.histogram("request_latency_seconds"),
    }
  }

  fn record_response(&self, status: ProduceStatus) {
    match status {
      ProduceStatus::PRODUCE_STATUS_OK => self.responses_ok_total.inc(),
      ProduceStatus::PRODUCE_STATUS_NOT_LEASE_HOLDER => {
        self.responses_not_lease_holder_total.inc();
      },
      ProduceStatus::PRODUCE_STATUS_UNKNOWN_TOPIC => {
        self.responses_unknown_topic_total.inc();
      },
      ProduceStatus::PRODUCE_STATUS_OVERLOADED => {
        self.responses_overloaded_total.inc();
      },
    }
  }
}

pub struct BrokerGrpc {
  write_engine: Arc<dyn WriteEngine>,
  metrics: BrokerGrpcMetrics,
}

impl BrokerGrpc {
  #[must_use]
  pub fn new(write_engine: Arc<dyn WriteEngine>, metrics_scope: &Scope) -> Self {
    Self {
      write_engine,
      metrics: BrokerGrpcMetrics::new(metrics_scope),
    }
  }
}

#[async_trait::async_trait]
impl Handler<ProduceBatchRequest, ProduceBatchResponse> for BrokerGrpc {
  async fn handle(
    &self,
    _headers: HeaderMap,
    _extensions: Extensions,
    request: ProduceBatchRequest,
  ) -> bd_grpc::error::Result<ProduceBatchResponse> {
    let started = Instant::now();
    self.metrics.requests_total.inc();

    let record_count = request.records.len();
    self.metrics.records_total.inc_by(record_count as u64);
    trace!(
      "broker received produce request: topic={}, virtual_partition_id={}, records={}",
      request.topic, request.virtual_partition_id, record_count
    );

    let write_request = WriteRequest {
      topic: request.topic.to_string(),
      virtual_partition_id: request.virtual_partition_id,
      records: request
        .records
        .into_iter()
        .map(|record| Record::new(record.payload.to_vec(), record.event_ts_ms))
        .collect(),
    };

    let result = self.write_engine.produce_batch(write_request).await;

    let status = match &result {
      Ok(_) => ProduceStatus::PRODUCE_STATUS_OK,
      Err(error) => error.status(),
    };

    self.metrics.record_response(status);
    self
      .metrics
      .request_latency_seconds
      .observe(started.elapsed().as_secs_f64());

    let response = match result {
      Ok(_) => ProduceBatchResponse {
        status: status.into(),
        error_message: String::new().into(),
        ..Default::default()
      },
      Err(error) => ProduceBatchResponse {
        status: status.into(),
        error_message: error_message(&error).into(),
        ..Default::default()
      },
    };

    Ok(response)
  }
}

pub fn make_broker_router(write_engine: Arc<dyn WriteEngine>, metrics: &BrokerMetrics) -> Router {
  let service_method = ServiceMethod::new("BrokerService", "ProduceBatch");
  let grpc_metrics_scope = metrics.scope();
  let admin_metrics = metrics.clone();
  bd_grpc::make_unary_router(
    &service_method,
    Arc::new(BrokerGrpc::new(write_engine, &grpc_metrics_scope)),
    |error| {
      warn_every!(15.seconds(), "broker gRPC handler error: {}", error);
    },
    None,
    true,
  )
  .route(
    "/metrics",
    get(move || {
      let metrics = admin_metrics.clone();
      async move {
        (
          [(CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
          metrics.prometheus_output(),
        )
      }
    }),
  )
  .route("/admin/log", post(log))
}

// Handler for /admin/log. Allows changing the active log level.
async fn log(Query(params): Query<HashMap<String, String>>) {
  if let Some(rust_log) = params.get("rust_log") {
    let _ = SwapLogger::swap(rust_log);
  }
}

fn error_message(error: &WriteError) -> String {
  match error {
    WriteError::Internal(inner) => inner.to_string(),
    other => other.to_string(),
  }
}
