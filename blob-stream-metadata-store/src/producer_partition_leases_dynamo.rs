#[cfg(test)]
#[path = "./producer_partition_leases_dynamo_test.rs"]
mod tests;

use crate::{
  LeaseAcquireAndReserveOutcome,
  LeaseAcquireOutcome,
  LeaseHeartbeatOutcome,
  LeaseReleaseOutcome,
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
use log::{debug, trace};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

const ATTR_PK: &str = "pk";
const ATTR_HOLDER: &str = "holder_id";
const ATTR_EXPIRES: &str = "lease_expiration_ts_ms";
const ATTR_MAX_SEQ: &str = "max_allocated_seq";
const ATTR_TTL: &str = "ttl_epoch_seconds";
const DEFAULT_LEASE_TTL_BUFFER_SECONDS: u32 = 3_600;

//
// DynamoProducerPartitionLeaseStore
//

#[derive(Clone, Debug)]
pub struct DynamoProducerPartitionLeaseStore {
  client: Client,
  table_name: String,
  ttl_buffer_seconds: i64,
}

impl DynamoProducerPartitionLeaseStore {
  #[must_use]
  pub fn new(client: Client, table_name: impl Into<String>) -> Self {
    Self::with_ttl_buffer_seconds(client, table_name, DEFAULT_LEASE_TTL_BUFFER_SECONDS)
  }

  #[must_use]
  pub fn with_ttl_buffer_seconds(
    client: Client,
    table_name: impl Into<String>,
    ttl_buffer_seconds: u32,
  ) -> Self {
    Self {
      client,
      table_name: table_name.into(),
      ttl_buffer_seconds: i64::from(ttl_buffer_seconds),
    }
  }

  async fn read_lease(
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
    Ok(Some(entry.into_lease(key.clone())))
  }

  fn lease_from_item(
    attributes: HashMap<String, AttributeValue>,
    key: ProducerPartitionLeaseKey,
  ) -> Result<ProducerPartitionLease> {
    let entry: DynamoLeaseItem = serde_dynamo::from_item(attributes)?;
    Ok(entry.into_lease(key))
  }
}

#[async_trait]
impl ProducerPartitionLeaseStore for DynamoProducerPartitionLeaseStore {
  async fn get_lease(
    &self,
    key: &ProducerPartitionLeaseKey,
  ) -> Result<Option<ProducerPartitionLease>> {
    self.read_lease(key).await
  }

  async fn acquire_lease(
    &self,
    key: ProducerPartitionLeaseKey,
    holder_id: String,
    now_ts_ms: i64,
    lease_duration_ms: i64,
  ) -> Result<LeaseAcquireOutcome> {
    match self
      .acquire_lease_and_reserve_sequences(key, holder_id, now_ts_ms, lease_duration_ms, None)
      .await?
    {
      LeaseAcquireAndReserveOutcome::Acquired {
        lease,
        reservation: None,
      } => Ok(LeaseAcquireOutcome::Acquired(lease)),
      LeaseAcquireAndReserveOutcome::Acquired {
        reservation: Some(_),
        ..
      } => {
        unreachable!("lease acquisition did not request a sequence reservation")
      },
      LeaseAcquireAndReserveOutcome::HeldByOther(lease) => {
        Ok(LeaseAcquireOutcome::HeldByOther(lease))
      },
    }
  }

  async fn acquire_lease_and_reserve_sequences(
    &self,
    key: ProducerPartitionLeaseKey,
    holder_id: String,
    now_ts_ms: i64,
    lease_duration_ms: i64,
    reservation_size: Option<u64>,
  ) -> Result<LeaseAcquireAndReserveOutcome> {
    trace!(
      "producer lease(dynamo) acquire/reserve: table={}, topic={}, partition={}, holder_id={}, \
       reservation_size={reservation_size:?}",
      self.table_name, key.topic, key.virtual_partition_id, holder_id
    );
    if reservation_size == Some(0) {
      return Err(anyhow!("reservation_size must be greater than zero"));
    }
    let expires_at = expires_at(now_ts_ms, lease_duration_ms)?;
    let ttl_epoch_seconds = ttl_epoch_seconds(expires_at, self.ttl_buffer_seconds)?;
    let pk = key.format();

    let mut values = HashMap::new();
    values.insert(":holder".to_string(), AttributeValue::S(holder_id.clone()));
    values.insert(
      ":expires".to_string(),
      AttributeValue::N(expires_at.to_string()),
    );
    values.insert(
      ":ttl".to_string(),
      AttributeValue::N(ttl_epoch_seconds.to_string()),
    );
    values.insert(":now".to_string(), AttributeValue::N(now_ts_ms.to_string()));
    if let Some(reservation_size) = reservation_size {
      values.insert(
        ":delta".to_string(),
        AttributeValue::N(reservation_size.to_string()),
      );
      values.insert(":initial".to_string(), AttributeValue::N("-1".to_string()));
    }

    let update = match reservation_size {
      Some(_) => format!(
        "SET {ATTR_HOLDER} = :holder, {ATTR_EXPIRES} = :expires, {ATTR_TTL} = :ttl, \
         {ATTR_MAX_SEQ} = if_not_exists({ATTR_MAX_SEQ}, :initial) + :delta"
      ),
      None => {
        format!("SET {ATTR_HOLDER} = :holder, {ATTR_EXPIRES} = :expires, {ATTR_TTL} = :ttl")
      },
    };
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
        let lease = Self::lease_from_item(attributes, key.clone())?;
        let reservation = match reservation_size {
          Some(size) => Some(reservation_from_new_max(
            lease
              .max_allocated_seq
              .ok_or_else(|| anyhow!("reserved lease missing max allocated sequence"))?,
            size,
          )?),
          None => None,
        };
        debug!(
          "producer lease(dynamo) acquire/reserve result: acquired, reservation={reservation:?}"
        );
        Ok(LeaseAcquireAndReserveOutcome::Acquired { lease, reservation })
      },
      Err(SdkError::ServiceError(service_error))
        if service_error.err().is_conditional_check_failed_exception() =>
      {
        debug!("producer lease(dynamo) acquire/reserve result: held_by_other");
        let lease = self
          .read_lease(&key)
          .await?
          .ok_or_else(|| anyhow!("lease missing after conditional failure"))?;
        Ok(LeaseAcquireAndReserveOutcome::HeldByOther(lease))
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
    trace!(
      "producer lease(dynamo) heartbeat: table={}, topic={}, partition={}, holder_id={}",
      self.table_name, key.topic, key.virtual_partition_id, holder_id
    );
    let expires_at = expires_at(now_ts_ms, lease_duration_ms)?;
    let ttl_epoch_seconds = ttl_epoch_seconds(expires_at, self.ttl_buffer_seconds)?;

    let mut values = HashMap::new();
    values.insert(
      ":holder".to_string(),
      AttributeValue::S(holder_id.to_string()),
    );
    values.insert(
      ":expires".to_string(),
      AttributeValue::N(expires_at.to_string()),
    );
    values.insert(
      ":ttl".to_string(),
      AttributeValue::N(ttl_epoch_seconds.to_string()),
    );
    values.insert(":now".to_string(), AttributeValue::N(now_ts_ms.to_string()));

    let update = format!("SET {ATTR_EXPIRES} = :expires, {ATTR_TTL} = :ttl");
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
        let lease = Self::lease_from_item(attributes, key.clone())?;
        debug!("producer lease(dynamo) heartbeat result: renewed");
        Ok(LeaseHeartbeatOutcome::Renewed(lease))
      },
      Err(SdkError::ServiceError(service_error))
        if service_error.err().is_conditional_check_failed_exception() =>
      {
        let Some(lease) = self.read_lease(key).await? else {
          debug!("producer lease(dynamo) heartbeat result: expired");
          return Ok(LeaseHeartbeatOutcome::Expired);
        };
        if lease.lease_expiration_ts_ms <= now_ts_ms {
          debug!("producer lease(dynamo) heartbeat result: expired");
          Ok(LeaseHeartbeatOutcome::Expired)
        } else {
          debug!("producer lease(dynamo) heartbeat result: held_by_other");
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
    reservation_size: u64,
  ) -> Result<SequenceReservationOutcome> {
    trace!(
      "producer lease(dynamo) reserve: table={}, topic={}, partition={}, holder_id={}, size={}",
      self.table_name, key.topic, key.virtual_partition_id, holder_id, reservation_size
    );
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
          .read_lease(key)
          .await?
          .ok_or_else(|| anyhow!("lease missing after reservation"))?;

        debug!(
          "producer lease(dynamo) reserve result: start={}, end={}",
          reservation.start, reservation.end
        );
        Ok(SequenceReservationOutcome::Reserved(SequenceReservation {
          range: reservation,
          lease,
        }))
      },
      Err(SdkError::ServiceError(service_error))
        if service_error.err().is_conditional_check_failed_exception() =>
      {
        let Some(lease) = self.read_lease(key).await? else {
          debug!("producer lease(dynamo) reserve result: expired");
          return Ok(SequenceReservationOutcome::Expired);
        };
        if lease.lease_expiration_ts_ms <= now_ts_ms {
          debug!("producer lease(dynamo) reserve result: expired");
          Ok(SequenceReservationOutcome::Expired)
        } else {
          debug!("producer lease(dynamo) reserve result: held_by_other");
          Ok(SequenceReservationOutcome::HeldByOther(lease))
        }
      },
      Err(error) => Err(error.into()),
    }
  }

  async fn release_lease(
    &self,
    key: &ProducerPartitionLeaseKey,
    holder_id: &str,
    now_ts_ms: i64,
  ) -> Result<LeaseReleaseOutcome> {
    trace!(
      "producer lease(dynamo) release: table={}, topic={}, partition={}, holder_id={}",
      self.table_name, key.topic, key.virtual_partition_id, holder_id
    );
    let mut values = HashMap::new();
    values.insert(
      ":holder".to_string(),
      AttributeValue::S(holder_id.to_string()),
    );
    values.insert(
      ":expired".to_string(),
      AttributeValue::N(now_ts_ms.to_string()),
    );
    values.insert(
      ":ttl".to_string(),
      AttributeValue::N(ttl_epoch_seconds(now_ts_ms, self.ttl_buffer_seconds)?.to_string()),
    );

    let update = format!("SET {ATTR_EXPIRES} = :expired, {ATTR_TTL} = :ttl");
    let condition = format!("{ATTR_HOLDER} = :holder");
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
      Ok(_) => {
        debug!("producer lease(dynamo) release result: released");
        Ok(LeaseReleaseOutcome::Released)
      },
      Err(SdkError::ServiceError(service_error))
        if service_error.err().is_conditional_check_failed_exception() =>
      {
        let Some(lease) = self.get_lease(key).await? else {
          debug!("producer lease(dynamo) release result: expired");
          return Ok(LeaseReleaseOutcome::Expired);
        };
        if lease.lease_expiration_ts_ms <= now_ts_ms {
          debug!("producer lease(dynamo) release result: expired");
          Ok(LeaseReleaseOutcome::Expired)
        } else {
          debug!("producer lease(dynamo) release result: held_by_other");
          Ok(LeaseReleaseOutcome::HeldByOther(lease))
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
  holder_id: String,
  lease_expiration_ts_ms: i64,
  max_allocated_seq: Option<u64>,
}

impl DynamoLeaseItem {
  fn into_lease(self, key: ProducerPartitionLeaseKey) -> ProducerPartitionLease {
    ProducerPartitionLease {
      key,
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

fn reservation_from_new_max(max_allocated_seq: u64, reservation_size: u64) -> Result<SeqRange> {
  let start = max_allocated_seq
    .checked_sub(reservation_size.saturating_sub(1))
    .ok_or_else(|| anyhow!("sequence range underflow"))?;
  Ok(SeqRange {
    start,
    end: max_allocated_seq,
  })
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

fn ttl_epoch_seconds(expires_at_ms: i64, ttl_buffer_seconds: i64) -> Result<i64> {
  let expires_at_seconds = expires_at_ms
    .checked_div(1_000)
    .ok_or_else(|| anyhow!("lease ttl conversion overflow"))?;

  expires_at_seconds
    .checked_add(ttl_buffer_seconds)
    .ok_or_else(|| anyhow!("lease ttl overflow"))
}
