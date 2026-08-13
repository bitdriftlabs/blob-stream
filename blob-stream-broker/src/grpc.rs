#[cfg(test)]
#[path = "./grpc_test.rs"]
mod tests;

use crate::metrics::BrokerMetrics;
use crate::write::{ProduceOutcomeMetrics, WriteEngine, WriteError, WriteRequest};
use axum::extract::Query;
use axum::http::header::CONTENT_TYPE;
use axum::routing::{get, post};
use axum::{Json, Router};
use bd_grpc::service::ServiceMethod;
use bd_grpc::{Handler, UnaryRequestConfig, UnaryRouterBuilder, ValidationOptions};
use bd_log::SwapLogger;
use bd_log_util::warn_every;
use bd_server_stats::stats::Scope;
use blob_stream_proto::protos::blobstream::v1::broker::{
  ProduceBatchRequest,
  ProduceBatchResponse,
  ProduceBatchesRequest,
  ProduceBatchesResponse,
  ProduceStatus,
};
use blob_stream_types::MAX_PRODUCE_BATCHES_REQUEST_BYTES;
use futures::{StreamExt, stream};
use http::{Extensions, HeaderMap};
use log::trace;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use time::ext::NumericalDuration;

const MAX_CONCURRENT_BATCHES_PER_GROUPED_REQUEST: usize = 16;

//
// BrokerGrpcMetrics
//

struct BrokerGrpcMetrics {
  rpc_requests_total: prometheus::IntCounter,
  batches_total: prometheus::IntCounter,
  records_total: prometheus::IntCounter,
  responses_ok_total: prometheus::IntCounter,
  responses_not_lease_holder_total: prometheus::IntCounter,
  responses_unknown_topic_total: prometheus::IntCounter,
  responses_overloaded_total: prometheus::IntCounter,
  responses_bad_request_total: prometheus::IntCounter,
  request_timeouts_total: prometheus::IntCounter,
  active_batches: prometheus::IntGauge,
  request_latency_seconds: prometheus::Histogram,
}

impl BrokerGrpcMetrics {
  fn new(scope: &Scope) -> Self {
    let scope = scope.scope("grpc");
    Self {
      rpc_requests_total: scope.counter("requests_total"),
      batches_total: scope.counter("batches_total"),
      records_total: scope.counter("records_total"),
      responses_ok_total: scope.counter("responses_ok_total"),
      responses_not_lease_holder_total: scope.counter("responses_not_lease_holder_total"),
      responses_unknown_topic_total: scope.counter("responses_unknown_topic_total"),
      responses_overloaded_total: scope.counter("responses_overloaded_total"),
      responses_bad_request_total: scope.counter("responses_bad_request_total"),
      request_timeouts_total: scope.counter("request_timeouts_total"),
      active_batches: scope.gauge("active_batches"),
      request_latency_seconds: scope.histogram("request_latency_seconds"),
    }
  }

  fn record_batch(&self, record_count: usize) {
    self.batches_total.inc();
    self.records_total.inc_by(record_count as u64);
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
      ProduceStatus::PRODUCE_STATUS_BAD_REQUEST => {
        self.responses_bad_request_total.inc();
      },
    }
  }

  fn record_timeout(&self) {
    self.request_timeouts_total.inc();
  }
}

pub struct BrokerGrpc {
  write_engine: Arc<dyn WriteEngine>,
  produce_request_timeout: Duration,
  metrics: BrokerGrpcMetrics,
  produce_outcomes: ProduceOutcomeMetrics,
}

impl BrokerGrpc {
  #[must_use]
  pub fn new(write_engine: Arc<dyn WriteEngine>, metrics_scope: &Scope) -> Self {
    let produce_request_timeout = write_engine.produce_request_timeout();
    Self {
      write_engine,
      produce_request_timeout,
      metrics: BrokerGrpcMetrics::new(metrics_scope),
      produce_outcomes: ProduceOutcomeMetrics::new(metrics_scope),
    }
  }

  async fn handle_batch(&self, request: ProduceBatchRequest) -> ProduceBatchResponse {
    let _active_batch = bd_server_stats::stats::StackAutoGauge::new(&self.metrics.active_batches);
    let started = Instant::now();
    let record_count = request.records.len();
    let payload_bytes = request
      .records
      .iter()
      .map(|record| record.payload.len() as u64)
      .sum::<u64>();
    self.metrics.record_batch(record_count);
    trace!(
      "broker received produce batch: topic={}, virtual_partition_id={}, records={}",
      request.topic, request.virtual_partition_id, record_count
    );

    let topic = request.topic;
    let virtual_partition_id = request.virtual_partition_id;
    // WriteState retains a topic as a map key across requests. Copy it out of the decoded RPC
    // buffer before handing it to the write path so one short topic cannot retain the full body.
    let write_request = WriteRequest {
      topic: topic.to_string().into(),
      virtual_partition_id,
      records: request.records,
    };

    // A stalled backend must not keep an incoming RPC (and its allocation transition) alive
    // indefinitely. Dropping this future releases the transition through its cancellation cleanup.
    let write_started = Instant::now();
    let result = match tokio::time::timeout(
      self.produce_request_timeout,
      self.write_engine.produce_batch(write_request),
    )
    .await
    {
      Ok(result) => result,
      Err(_elapsed) => {
        self.metrics.record_timeout();
        warn_every!(
          15.seconds(),
          "broker produce request timed out: topic={topic}, \
           virtual_partition_id={virtual_partition_id}, records={record_count}, timeout_ms={}",
          self.produce_request_timeout.as_millis(),
        );
        let error = WriteError::Overloaded(format!(
          "produce request timed out after {} ms",
          self.produce_request_timeout.as_millis()
        ));
        let status = error.status();
        self.metrics.record_response(status);
        self
          .metrics
          .request_latency_seconds
          .observe(started.elapsed().as_secs_f64());
        return ProduceBatchResponse {
          status: status.into(),
          error_message: error_message(&error).into(),
          ..Default::default()
        };
      },
    };

    self.produce_outcomes.record_result(
      &result,
      record_count as u64,
      payload_bytes,
      write_started.elapsed(),
    );

    let status = match &result {
      Ok(_) => ProduceStatus::PRODUCE_STATUS_OK,
      Err(error) => error.status(),
    };

    self.metrics.record_response(status);
    self
      .metrics
      .request_latency_seconds
      .observe(started.elapsed().as_secs_f64());

    match result {
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
    self.metrics.rpc_requests_total.inc();
    Ok(self.handle_batch(request).await)
  }
}

#[async_trait::async_trait]
impl Handler<ProduceBatchesRequest, ProduceBatchesResponse> for BrokerGrpc {
  async fn handle(
    &self,
    _headers: HeaderMap,
    _extensions: Extensions,
    request: ProduceBatchesRequest,
  ) -> bd_grpc::error::Result<ProduceBatchesResponse> {
    self.metrics.rpc_requests_total.inc();
    // A byte-bounded grouped request can still contain many small logical batches. Bound their
    // write-engine work while `buffered` retains the request order required by the response.
    let results = stream::iter(request.batches)
      .map(|batch| self.handle_batch(batch))
      .buffered(MAX_CONCURRENT_BATCHES_PER_GROUPED_REQUEST)
      .collect()
      .await;
    Ok(ProduceBatchesResponse {
      results,
      ..Default::default()
    })
  }
}

pub fn make_broker_router(write_engine: Arc<dyn WriteEngine>, metrics: &BrokerMetrics) -> Router {
  let produce_batch_method = ServiceMethod::<ProduceBatchRequest, ProduceBatchResponse>::new(
    "BrokerService",
    "ProduceBatch",
  );
  let produce_batches_method = ServiceMethod::<ProduceBatchesRequest, ProduceBatchesResponse>::new(
    "BrokerService",
    "ProduceBatches",
  );
  let grpc_metrics_scope = metrics.scope();
  let admin_metrics = metrics.clone();
  let admin_write_engine = write_engine.clone();
  let grpc = Arc::new(BrokerGrpc::new(write_engine, &grpc_metrics_scope));
  let produce_batch_router = UnaryRouterBuilder::new(&produce_batch_method, grpc.clone())
    .request_config(produce_request_config())
    .error_handler(|error| {
      warn_every!(15.seconds(), "broker gRPC handler error: {error}");
    })
    .build()
    .expect("legacy broker gRPC router should build");
  let produce_batches_router = UnaryRouterBuilder::new(&produce_batches_method, grpc)
    .request_config(produce_request_config())
    .error_handler(|error| {
      warn_every!(15.seconds(), "broker gRPC handler error: {error}");
    })
    .build()
    .expect("batched broker gRPC router should build");
  produce_batch_router
    .merge(produce_batches_router)
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
    .route(
      "/admin/state",
      get(move || {
        let write_engine = admin_write_engine.clone();
        async move { Json(write_engine.state_snapshot().await) }
      }),
    )
    .route("/admin/log", post(log))
}

fn produce_request_config() -> UnaryRequestConfig {
  UnaryRequestConfig {
    max_decoded_request_bytes: MAX_PRODUCE_BATCHES_REQUEST_BYTES,
    ..UnaryRequestConfig::default()
  }
  .with_validation_options(ValidationOptions::default())
}

// Handler for /admin/log. Allows changing the active log level.
async fn log(Query(params): Query<HashMap<String, String>>) {
  if let Some(rust_log) = params.get("rust_log") {
    let _ = SwapLogger::swap(rust_log);
  }
}

fn error_message(error: &WriteError) -> String {
  match error {
    WriteError::Internal(_) => "internal write failure".to_string(),
    other => other.to_string(),
  }
}
