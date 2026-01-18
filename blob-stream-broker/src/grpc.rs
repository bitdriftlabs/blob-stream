// blob-stream - gRPC wiring
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use axum::Router;
use bd_grpc::Handler;
use bd_grpc::service::ServiceMethod;
use bd_log::warn_every;
use blob_stream_proto::protos::blobstream::v1::broker::{
  ProduceBatchRequest,
  ProduceBatchResponse,
  ProduceStatus,
};
use http::{Extensions, HeaderMap};
use std::sync::Arc;
use time::ext::NumericalDuration;

pub struct BrokerGrpc;

#[async_trait::async_trait]
impl Handler<ProduceBatchRequest, ProduceBatchResponse> for BrokerGrpc {
  async fn handle(
    &self,
    _headers: HeaderMap,
    _extensions: Extensions,
    _request: ProduceBatchRequest,
  ) -> bd_grpc::error::Result<ProduceBatchResponse> {
    Ok(ProduceBatchResponse {
      status: ProduceStatus::PRODUCE_STATUS_OK.into(),
      error_message: String::new().into(),
      ..Default::default()
    })
  }
}

pub fn make_broker_router() -> Router {
  let service_method = ServiceMethod::new("BrokerService", "ProduceBatch");
  bd_grpc::make_unary_router(
    &service_method,
    Arc::new(BrokerGrpc),
    |error| {
      warn_every!(15.seconds(), "broker gRPC handler error: {}", error);
    },
    None,
    true,
  )
}
