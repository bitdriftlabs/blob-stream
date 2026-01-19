// blob-stream - DynamoDB producer partition leases
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./producer_partition_leases_dynamo_test.rs"]
mod tests;

use crate::{
  LeaseAcquireOutcome,
  LeaseHeartbeatOutcome,
  ProducerPartitionLease,
  ProducerPartitionLeaseKey,
  ProducerPartitionLeaseStore,
  SequenceReservation,
  SequenceReservationOutcome,
};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::types::{AttributeValue, ReturnValue};
use blob_stream_types::SeqRange;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

const ATTR_PK: &str = "pk";
const ATTR_HOLDER: &str = "holder_id";
const ATTR_EXPIRES: &str = "lease_expiration_ts_ms";
const ATTR_MAX_SEQ: &str = "max_allocated_seq";
const ATTR_TOPIC: &str = "topic";
const ATTR_WRITER_ID: &str = "writer_id";
const ATTR_VIRTUAL_PARTITION_ID: &str = "virtual_partition_id";

//
// DynamoProducerPartitionLeaseStore
//

#[derive(Clone, Debug)]
pub struct DynamoProducerPartitionLeaseStore {
  client: Client,
  table_name: String,
}

impl DynamoProducerPartitionLeaseStore {
  #[must_use]
  pub fn new(client: Client, table_name: impl Into<String>) -> Self {
    Self {
      client,
      table_name: table_name.into(),
    }
  }

  async fn get_lease(
    &self,
    key: &ProducerPartitionLeaseKey,
  ) -> Result<Option<ProducerPartitionLease>> {
    let response = self
      .client
      .get_item()
      .table_name(&self.table_name)
      .key(ATTR_PK, AttributeValue::S(key.format()))
      .consistent_read(false)
      .send()
      .await?;

    let Some(item) = response.item else {
      return Ok(None);
    };

    let entry: DynamoLeaseItem = serde_dynamo::from_item(item)?;
    Ok(Some(entry.into_lease()))
  }

  fn lease_from_item(
    attributes: HashMap<String, AttributeValue>,
  ) -> Result<ProducerPartitionLease> {
    let entry: DynamoLeaseItem = serde_dynamo::from_item(attributes)?;
    Ok(entry.into_lease())
  }
}

#[async_trait]
impl ProducerPartitionLeaseStore for DynamoProducerPartitionLeaseStore {
  async fn acquire_lease(
    &self,
    key: ProducerPartitionLeaseKey,
    holder_id: String,
    now_ts_ms: i64,
    lease_duration_ms: i64,
  ) -> Result<LeaseAcquireOutcome> {
    let expires_at = expires_at(now_ts_ms, lease_duration_ms)?;
    let pk = key.format();

    let mut values = HashMap::new();
    values.insert(":holder".to_string(), AttributeValue::S(holder_id.clone()));
    values.insert(
      ":expires".to_string(),
      AttributeValue::N(expires_at.to_string()),
    );
    values.insert(":now".to_string(), AttributeValue::N(now_ts_ms.to_string()));
    values.insert(":topic".to_string(), AttributeValue::S(key.topic.clone()));
    values.insert(
      ":writer_id".to_string(),
      AttributeValue::N(key.writer_id.to_string()),
    );
    values.insert(
      ":virtual_partition_id".to_string(),
      AttributeValue::N(key.virtual_partition_id.to_string()),
    );

    let update = format!(
      "SET {ATTR_HOLDER} = :holder, {ATTR_EXPIRES} = :expires, {ATTR_TOPIC} = \
       if_not_exists({ATTR_TOPIC}, :topic), {ATTR_WRITER_ID} = if_not_exists({ATTR_WRITER_ID}, \
       :writer_id), {ATTR_VIRTUAL_PARTITION_ID} = if_not_exists({ATTR_VIRTUAL_PARTITION_ID}, \
       :virtual_partition_id)"
    );
    let condition = format!(
      "attribute_not_exists({ATTR_PK}) OR {ATTR_EXPIRES} <= :now OR {ATTR_HOLDER} = :holder"
    );

    let response = self
      .client
      .update_item()
      .table_name(&self.table_name)
      .key(ATTR_PK, AttributeValue::S(pk))
      .update_expression(update)
      .condition_expression(condition)
      .set_expression_attribute_values(Some(values))
      .return_values(ReturnValue::AllNew)
      .send()
      .await;

    match response {
      Ok(output) => {
        let attributes = output
          .attributes
          .ok_or_else(|| anyhow!("lease attributes missing"))?;
        let lease = Self::lease_from_item(attributes)?;
        Ok(LeaseAcquireOutcome::Acquired(lease))
      },
      Err(SdkError::ServiceError(service_error))
        if service_error.err().is_conditional_check_failed_exception() =>
      {
        let lease = self
          .get_lease(&key)
          .await?
          .ok_or_else(|| anyhow!("lease missing after conditional failure"))?;
        Ok(LeaseAcquireOutcome::HeldByOther(lease))
      },
      Err(error) => Err(error.into()),
    }
  }

  async fn heartbeat_lease(
    &self,
    key: &ProducerPartitionLeaseKey,
    holder_id: &str,
    now_ts_ms: i64,
    lease_duration_ms: i64,
  ) -> Result<LeaseHeartbeatOutcome> {
    let expires_at = expires_at(now_ts_ms, lease_duration_ms)?;

    let mut values = HashMap::new();
    values.insert(
      ":holder".to_string(),
      AttributeValue::S(holder_id.to_string()),
    );
    values.insert(
      ":expires".to_string(),
      AttributeValue::N(expires_at.to_string()),
    );
    values.insert(":now".to_string(), AttributeValue::N(now_ts_ms.to_string()));

    let update = format!("SET {ATTR_EXPIRES} = :expires");
    let condition = format!("{ATTR_HOLDER} = :holder AND {ATTR_EXPIRES} > :now");

    let response = self
      .client
      .update_item()
      .table_name(&self.table_name)
      .key(ATTR_PK, AttributeValue::S(key.format()))
      .update_expression(update)
      .condition_expression(condition)
      .set_expression_attribute_values(Some(values))
      .return_values(ReturnValue::AllNew)
      .send()
      .await;

    match response {
      Ok(output) => {
        let attributes = output
          .attributes
          .ok_or_else(|| anyhow!("lease attributes missing"))?;
        let lease = Self::lease_from_item(attributes)?;
        Ok(LeaseHeartbeatOutcome::Renewed(lease))
      },
      Err(SdkError::ServiceError(service_error))
        if service_error.err().is_conditional_check_failed_exception() =>
      {
        let Some(lease) = self.get_lease(key).await? else {
          return Ok(LeaseHeartbeatOutcome::Expired);
        };
        if lease.lease_expiration_ts_ms <= now_ts_ms {
          Ok(LeaseHeartbeatOutcome::Expired)
        } else {
          Ok(LeaseHeartbeatOutcome::HeldByOther(lease))
        }
      },
      Err(error) => Err(error.into()),
    }
  }

  async fn reserve_sequences(
    &self,
    key: &ProducerPartitionLeaseKey,
    holder_id: &str,
    now_ts_ms: i64,
    _lease_duration_ms: i64,
    reservation_size: u64,
  ) -> Result<SequenceReservationOutcome> {
    if reservation_size == 0 {
      return Err(anyhow!("reservation_size must be greater than zero"));
    }

    let mut values = HashMap::new();
    values.insert(
      ":holder".to_string(),
      AttributeValue::S(holder_id.to_string()),
    );
    values.insert(":now".to_string(), AttributeValue::N(now_ts_ms.to_string()));
    values.insert(
      ":delta".to_string(),
      AttributeValue::N(reservation_size.to_string()),
    );
    values.insert(":initial".to_string(), AttributeValue::N("-1".to_string()));

    let update = format!("SET {ATTR_MAX_SEQ} = if_not_exists({ATTR_MAX_SEQ}, :initial) + :delta");
    let condition = format!("{ATTR_HOLDER} = :holder AND {ATTR_EXPIRES} > :now");

    let response = self
      .client
      .update_item()
      .table_name(&self.table_name)
      .key(ATTR_PK, AttributeValue::S(key.format()))
      .update_expression(update)
      .condition_expression(condition)
      .set_expression_attribute_values(Some(values))
      .return_values(ReturnValue::UpdatedOld)
      .send()
      .await;

    match response {
      Ok(output) => {
        let old_max = output
          .attributes
          .as_ref()
          .and_then(|attrs| attrs.get(ATTR_MAX_SEQ))
          .map(parse_i64)
          .transpose()?
          .unwrap_or(-1);

        let reservation = reservation_from_old(old_max, reservation_size)?;
        let lease = self
          .get_lease(key)
          .await?
          .ok_or_else(|| anyhow!("lease missing after reservation"))?;

        Ok(SequenceReservationOutcome::Reserved(SequenceReservation {
          range: reservation,
          lease,
        }))
      },
      Err(SdkError::ServiceError(service_error))
        if service_error.err().is_conditional_check_failed_exception() =>
      {
        let Some(lease) = self.get_lease(key).await? else {
          return Ok(SequenceReservationOutcome::Expired);
        };
        if lease.lease_expiration_ts_ms <= now_ts_ms {
          Ok(SequenceReservationOutcome::Expired)
        } else {
          Ok(SequenceReservationOutcome::HeldByOther(lease))
        }
      },
      Err(error) => Err(error.into()),
    }
  }
}

//
// DynamoLeaseItem
//

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DynamoLeaseItem {
  #[serde(rename = "pk")]
  partition_key: String,
  topic: String,
  writer_id: u32,
  virtual_partition_id: u32,
  holder_id: String,
  lease_expiration_ts_ms: i64,
  max_allocated_seq: Option<u64>,
}

impl DynamoLeaseItem {
  fn into_lease(self) -> ProducerPartitionLease {
    ProducerPartitionLease {
      key: ProducerPartitionLeaseKey {
        topic: self.topic,
        writer_id: self.writer_id,
        virtual_partition_id: self.virtual_partition_id,
      },
      holder_id: self.holder_id,
      lease_expiration_ts_ms: self.lease_expiration_ts_ms,
      max_allocated_seq: self.max_allocated_seq,
    }
  }
}

fn reservation_from_old(old_max: i64, reservation_size: u64) -> Result<SeqRange> {
  if old_max < -1 {
    return Err(anyhow!("sequence range underflow"));
  }

  let start = if old_max < 0 {
    0
  } else {
    u64::try_from(old_max)
      .map_err(|error| anyhow!("sequence range overflow: {error}"))?
      .checked_add(1)
      .ok_or_else(|| anyhow!("sequence range overflow"))?
  };

  let end = start
    .checked_add(reservation_size.saturating_sub(1))
    .ok_or_else(|| anyhow!("sequence range overflow"))?;

  Ok(SeqRange { start, end })
}

fn parse_i64(value: &AttributeValue) -> Result<i64> {
  match value {
    AttributeValue::N(value) => value
      .parse::<i64>()
      .map_err(|error| anyhow!("invalid number {value}: {error}")),
    other => Err(anyhow!("unexpected attribute value {other:?}")),
  }
}

fn expires_at(now_ts_ms: i64, lease_duration_ms: i64) -> Result<i64> {
  now_ts_ms
    .checked_add(lease_duration_ms)
    .ok_or_else(|| anyhow!("lease expiration overflow"))
}
