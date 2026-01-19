// blob-stream - gRPC wiring
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::write::{WriteEngine, WriteError, WriteRequest};
use axum::Router;
use bd_grpc::Handler;
use bd_grpc::service::ServiceMethod;
use bd_log::warn_every;
use blob_stream_proto::protos::blobstream::v1::broker::{
  ProduceBatchRequest,
  ProduceBatchResponse,
  ProduceStatus,
};
use blob_stream_types::Record;
use http::{Extensions, HeaderMap};
use std::sync::Arc;
use time::ext::NumericalDuration;

pub struct BrokerGrpc {
  write_engine: Arc<dyn WriteEngine>,
}

impl BrokerGrpc {
  #[must_use]
  pub fn new(write_engine: Arc<dyn WriteEngine>) -> Self {
    Self { write_engine }
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

    let response = match result {
      Ok(_) => ProduceBatchResponse {
        status: ProduceStatus::PRODUCE_STATUS_OK.into(),
        error_message: String::new().into(),
        ..Default::default()
      },
      Err(error) => ProduceBatchResponse {
        status: error.status().into(),
        error_message: error_message(&error).into(),
        ..Default::default()
      },
    };

    Ok(response)
  }
}

pub fn make_broker_router(write_engine: Arc<dyn WriteEngine>) -> Router {
  let service_method = ServiceMethod::new("BrokerService", "ProduceBatch");
  bd_grpc::make_unary_router(
    &service_method,
    Arc::new(BrokerGrpc::new(write_engine)),
    |error| {
      warn_every!(15.seconds(), "broker gRPC handler error: {}", error);
    },
    None,
    true,
  )
}

fn error_message(error: &WriteError) -> String {
  match error {
    WriteError::Internal(inner) => inner.to_string(),
    other => other.to_string(),
  }
}
