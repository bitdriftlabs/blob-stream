#[cfg(test)]
#[path = "./producer_partition_leases_dynamo_test.rs"]
mod tests;

use crate::aws::{is_dynamo_transaction_conflict, retry_dynamo_transaction_conflicts};
use crate::dynamo_attributes::{
  ATTR_EPOCH,
  ATTR_EXPIRES,
  ATTR_HOLDER,
  ATTR_PK,
  ATTR_SESSION,
  ATTR_TTL,
};
use crate::{
  DynamoCapacityMetrics,
  LeaseAcquireAndReserveOutcome,
  LeaseAcquireOutcome,
  LeaseHeartbeatOutcome,
  LeaseReleaseOutcome,
  ProducerLeaseFence,
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
use aws_sdk_dynamodb::operation::update_item::UpdateItemOutput;
use aws_sdk_dynamodb::types::{AttributeValue, ReturnConsumedCapacity, ReturnValue};
use blob_stream_types::{
  SeqRange,
  offset_datetime_from_unix_millis,
  unix_millis_from_offset_datetime,
};
use log::{debug, trace};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use time::{Duration, OffsetDateTime};

const ATTR_MAX_SEQ: &str = "max_allocated_seq";

//
// DynamoProducerPartitionLeaseStore
//

#[derive(Clone, Debug)]
pub struct DynamoProducerPartitionLeaseStore {
  client: Client,
  table_name: String,
  ttl_buffer_seconds: i64,
  capacity_metrics: Option<DynamoCapacityMetrics>,
}

impl DynamoProducerPartitionLeaseStore {
  #[must_use]
  pub fn new(
    client: Client,
    table_name: impl Into<String>,
    ttl_buffer_seconds: u32,
    capacity_metrics: Option<DynamoCapacityMetrics>,
  ) -> Self {
    Self {
      client,
      table_name: table_name.into(),
      ttl_buffer_seconds: i64::from(ttl_buffer_seconds),
      capacity_metrics,
    }
  }

  fn record_read_capacity(
    &self,
    consumed_capacity: Option<&aws_sdk_dynamodb::types::ConsumedCapacity>,
  ) {
    if let Some(capacity_metrics) = &self.capacity_metrics {
      capacity_metrics.record_read(consumed_capacity);
    }
  }

  fn record_write_capacity(
    &self,
    consumed_capacity: Option<&aws_sdk_dynamodb::types::ConsumedCapacity>,
  ) {
    if let Some(capacity_metrics) = &self.capacity_metrics {
      capacity_metrics.record_write(consumed_capacity);
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
      .return_consumed_capacity(ReturnConsumedCapacity::Total)
      .send()
      .await?;
    self.record_read_capacity(response.consumed_capacity.as_ref());

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

  // Both renewal and takeover requests return the complete updated lease. Decode that shared
  // response here so their reservation ranges, capacity accounting, and error behavior remain
  // identical.
  fn complete_acquire(
    &self,
    output: UpdateItemOutput,
    key: ProducerPartitionLeaseKey,
    reservation_size: Option<u64>,
    outcome: &str,
  ) -> Result<LeaseAcquireAndReserveOutcome> {
    self.record_write_capacity(output.consumed_capacity.as_ref());
    let attributes = output
      .attributes
      .ok_or_else(|| anyhow!("lease attributes missing"))?;
    let lease = Self::lease_from_item(attributes, key)?;
    let reservation = match reservation_size {
      Some(size) => Some(reservation_from_new_max(
        lease
          .max_allocated_seq
          .ok_or_else(|| anyhow!("reserved lease missing max allocated sequence"))?,
        size,
      )?),
      None => None,
    };
    debug!("producer lease(dynamo) acquire/reserve result: {outcome}, reservation={reservation:?}");
    Ok(LeaseAcquireAndReserveOutcome::Acquired { lease, reservation })
  }

  // A failed renewal can mean an absent or expired lease. A live lease remains owned by its
  // current holder and must be reported to the caller rather than overwritten.
  async fn claim_after_failed_renewal(
    &self,
    pk: String,
    mut values: HashMap<String, AttributeValue>,
    reservation_size: Option<u64>,
  ) -> Result<Option<UpdateItemOutput>> {
    values.insert(
      ":epoch_zero".to_string(),
      AttributeValue::N("0".to_string()),
    );
    values.insert(
      ":epoch_increment".to_string(),
      AttributeValue::N("1".to_string()),
    );
    let claim_update = match reservation_size {
      Some(_) => format!(
        "SET {ATTR_HOLDER} = :holder, {ATTR_SESSION} = :session, {ATTR_EXPIRES} = :expires, \
         {ATTR_TTL} = :ttl, {ATTR_EPOCH} = if_not_exists({ATTR_EPOCH}, :epoch_zero) + \
         :epoch_increment, {ATTR_MAX_SEQ} = if_not_exists({ATTR_MAX_SEQ}, :initial) + :delta"
      ),
      None => format!(
        "SET {ATTR_HOLDER} = :holder, {ATTR_SESSION} = :session, {ATTR_EXPIRES} = :expires, \
         {ATTR_TTL} = :ttl, {ATTR_EPOCH} = if_not_exists({ATTR_EPOCH}, :epoch_zero) + \
         :epoch_increment"
      ),
    };
    let claim_condition = format!("attribute_not_exists({ATTR_PK}) OR {ATTR_EXPIRES} <= :now");
    let claim_condition = match reservation_size {
      Some(_) => format!(
        "({claim_condition}) AND (attribute_not_exists({ATTR_MAX_SEQ}) OR {ATTR_MAX_SEQ} <= \
         :max_reservable)"
      ),
      None => claim_condition,
    };

    match retry_dynamo_transaction_conflicts(
      "producer_lease_claim",
      || {
        self
          .client
          .update_item()
          .table_name(&self.table_name)
          .key(ATTR_PK, AttributeValue::S(pk.clone()))
          .update_expression(claim_update.clone())
          .condition_expression(claim_condition.clone())
          .set_expression_attribute_values(Some(values.clone()))
          .return_values(ReturnValue::AllNew)
          .return_consumed_capacity(ReturnConsumedCapacity::Total)
          .send()
      },
      is_dynamo_transaction_conflict,
    )
    .await
    {
      Ok(output) => Ok(Some(output)),
      Err(SdkError::ServiceError(service_error))
        if service_error.err().is_conditional_check_failed_exception() =>
      {
        Ok(None)
      },
      Err(error) => Err(error.into()),
    }
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
    lease_session_id: String,
    now: OffsetDateTime,
    lease_duration: Duration,
  ) -> Result<LeaseAcquireOutcome> {
    match self
      .acquire_lease_and_reserve_sequences(
        key,
        holder_id,
        lease_session_id,
        now,
        lease_duration,
        None,
      )
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
    lease_session_id: String,
    now: OffsetDateTime,
    lease_duration: Duration,
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
    let expires_at = expires_at(now, lease_duration)?;
    let expires_at_ms = unix_millis_from_offset_datetime(expires_at)
      .map_err(|_| anyhow!("lease expiration exceeds Dynamo millisecond range"))?;
    let now_ts_ms = unix_millis_from_offset_datetime(now)
      .map_err(|_| anyhow!("current time exceeds Dynamo millisecond range"))?;
    let ttl_epoch_seconds = ttl_epoch_seconds(expires_at_ms, self.ttl_buffer_seconds)?;
    let pk = key.format();

    let mut values = HashMap::new();
    values.insert(":holder".to_string(), AttributeValue::S(holder_id.clone()));
    values.insert(
      ":session".to_string(),
      AttributeValue::S(lease_session_id.clone()),
    );
    values.insert(
      ":expires".to_string(),
      AttributeValue::N(expires_at_ms.to_string()),
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
      values.insert(
        ":max_reservable".to_string(),
        AttributeValue::N((u64::MAX - reservation_size).to_string()),
      );
    }

    let renewal_update = match reservation_size {
      Some(_) => format!(
        "SET {ATTR_EXPIRES} = :expires, {ATTR_TTL} = :ttl, {ATTR_MAX_SEQ} = \
         if_not_exists({ATTR_MAX_SEQ}, :initial) + :delta"
      ),
      None => format!("SET {ATTR_EXPIRES} = :expires, {ATTR_TTL} = :ttl"),
    };
    let renewal_condition =
      format!("{ATTR_HOLDER} = :holder AND {ATTR_SESSION} = :session AND {ATTR_EXPIRES} > :now");
    let renewal_condition = match reservation_size {
      Some(_) => format!(
        "({renewal_condition}) AND (attribute_not_exists({ATTR_MAX_SEQ}) OR {ATTR_MAX_SEQ} <= \
         :max_reservable)"
      ),
      None => renewal_condition,
    };
    let response = retry_dynamo_transaction_conflicts(
      "producer_lease_acquire_or_renew",
      || {
        self
          .client
          .update_item()
          .table_name(&self.table_name)
          .key(ATTR_PK, AttributeValue::S(pk.clone()))
          .update_expression(renewal_update.clone())
          .condition_expression(renewal_condition.clone())
          .set_expression_attribute_values(Some(values.clone()))
          .return_values(ReturnValue::AllNew)
          .return_consumed_capacity(ReturnConsumedCapacity::Total)
          .send()
      },
      is_dynamo_transaction_conflict,
    )
    .await;

    match response {
      Ok(output) => self.complete_acquire(output, key, reservation_size, "acquired"),
      Err(SdkError::ServiceError(service_error))
        if service_error.err().is_conditional_check_failed_exception() =>
      {
        if let Some(output) = self
          .claim_after_failed_renewal(pk, values, reservation_size)
          .await?
        {
          self.complete_acquire(output, key, reservation_size, "acquired takeover")
        } else {
          debug!("producer lease(dynamo) acquire/reserve result: held_by_other");
          let lease = self
            .read_lease(&key)
            .await?
            .ok_or_else(|| anyhow!("lease missing after conditional failure"))?;
          if reservation_would_overflow(
            &lease,
            &holder_id,
            &lease_session_id,
            now,
            reservation_size,
          ) {
            return Err(anyhow!("sequence range overflow"));
          }
          Ok(LeaseAcquireAndReserveOutcome::HeldByOther(lease))
        }
      },
      Err(error) => Err(error.into()),
    }
  }

  async fn heartbeat_lease(
    &self,
    key: &ProducerPartitionLeaseKey,
    holder_id: &str,
    lease_session_id: &str,
    now: OffsetDateTime,
    lease_duration: Duration,
  ) -> Result<LeaseHeartbeatOutcome> {
    trace!(
      "producer lease(dynamo) heartbeat: table={}, topic={}, partition={}, holder_id={}",
      self.table_name, key.topic, key.virtual_partition_id, holder_id
    );
    let expires_at = expires_at(now, lease_duration)?;
    let expires_at_ms = unix_millis_from_offset_datetime(expires_at)
      .map_err(|_| anyhow!("lease expiration exceeds Dynamo millisecond range"))?;
    let now_ts_ms = unix_millis_from_offset_datetime(now)
      .map_err(|_| anyhow!("current time exceeds Dynamo millisecond range"))?;
    let ttl_epoch_seconds = ttl_epoch_seconds(expires_at_ms, self.ttl_buffer_seconds)?;

    let mut values = HashMap::new();
    values.insert(
      ":holder".to_string(),
      AttributeValue::S(holder_id.to_string()),
    );
    values.insert(
      ":session".to_string(),
      AttributeValue::S(lease_session_id.to_string()),
    );
    values.insert(
      ":expires".to_string(),
      AttributeValue::N(expires_at_ms.to_string()),
    );
    values.insert(
      ":ttl".to_string(),
      AttributeValue::N(ttl_epoch_seconds.to_string()),
    );
    values.insert(":now".to_string(), AttributeValue::N(now_ts_ms.to_string()));

    let update = format!("SET {ATTR_EXPIRES} = :expires, {ATTR_TTL} = :ttl");
    let condition =
      format!("{ATTR_HOLDER} = :holder AND {ATTR_SESSION} = :session AND {ATTR_EXPIRES} > :now");

    let response = retry_dynamo_transaction_conflicts(
      "producer_lease_heartbeat",
      || {
        self
          .client
          .update_item()
          .table_name(&self.table_name)
          .key(ATTR_PK, AttributeValue::S(key.format()))
          .update_expression(update.clone())
          .condition_expression(condition.clone())
          .set_expression_attribute_values(Some(values.clone()))
          .return_values(ReturnValue::AllNew)
          .return_consumed_capacity(ReturnConsumedCapacity::Total)
          .send()
      },
      is_dynamo_transaction_conflict,
    )
    .await;

    match response {
      Ok(output) => {
        self.record_write_capacity(output.consumed_capacity.as_ref());
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
        if lease.lease_expiration_at <= now {
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
    lease_session_id: &str,
    now: OffsetDateTime,
    reservation_size: u64,
  ) -> Result<SequenceReservationOutcome> {
    trace!(
      "producer lease(dynamo) reserve: table={}, topic={}, partition={}, holder_id={}, size={}",
      self.table_name, key.topic, key.virtual_partition_id, holder_id, reservation_size
    );
    if reservation_size == 0 {
      return Err(anyhow!("reservation_size must be greater than zero"));
    }
    let now_ts_ms = unix_millis_from_offset_datetime(now)
      .map_err(|_| anyhow!("current time exceeds Dynamo millisecond range"))?;

    let mut values = HashMap::new();
    values.insert(
      ":holder".to_string(),
      AttributeValue::S(holder_id.to_string()),
    );
    values.insert(
      ":session".to_string(),
      AttributeValue::S(lease_session_id.to_string()),
    );
    values.insert(":now".to_string(), AttributeValue::N(now_ts_ms.to_string()));
    values.insert(
      ":delta".to_string(),
      AttributeValue::N(reservation_size.to_string()),
    );
    values.insert(":initial".to_string(), AttributeValue::N("-1".to_string()));
    values.insert(
      ":max_reservable".to_string(),
      AttributeValue::N((u64::MAX - reservation_size).to_string()),
    );

    let update = format!("SET {ATTR_MAX_SEQ} = if_not_exists({ATTR_MAX_SEQ}, :initial) + :delta");
    let condition = format!(
      "{ATTR_HOLDER} = :holder AND {ATTR_SESSION} = :session AND {ATTR_EXPIRES} > :now AND \
       (attribute_not_exists({ATTR_MAX_SEQ}) OR {ATTR_MAX_SEQ} <= :max_reservable)"
    );

    let response = retry_dynamo_transaction_conflicts(
      "producer_lease_reserve_sequences",
      || {
        self
          .client
          .update_item()
          .table_name(&self.table_name)
          .key(ATTR_PK, AttributeValue::S(key.format()))
          .update_expression(update.clone())
          .condition_expression(condition.clone())
          .set_expression_attribute_values(Some(values.clone()))
          .return_values(ReturnValue::UpdatedOld)
          .return_consumed_capacity(ReturnConsumedCapacity::Total)
          .send()
      },
      is_dynamo_transaction_conflict,
    )
    .await;

    match response {
      Ok(output) => {
        self.record_write_capacity(output.consumed_capacity.as_ref());
        let old_max = output
          .attributes
          .as_ref()
          .and_then(|attrs| attrs.get(ATTR_MAX_SEQ))
          .map(parse_u64)
          .transpose()?;

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
        if reservation_would_overflow(
          &lease,
          holder_id,
          lease_session_id,
          now,
          Some(reservation_size),
        ) {
          return Err(anyhow!("sequence range overflow"));
        }
        if lease.lease_expiration_at <= now {
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
    lease_session_id: &str,
    now: OffsetDateTime,
  ) -> Result<LeaseReleaseOutcome> {
    trace!(
      "producer lease(dynamo) release: table={}, topic={}, partition={}, holder_id={}",
      self.table_name, key.topic, key.virtual_partition_id, holder_id
    );
    let now_ts_ms = unix_millis_from_offset_datetime(now)
      .map_err(|_| anyhow!("current time exceeds Dynamo millisecond range"))?;
    let mut values = HashMap::new();
    values.insert(
      ":holder".to_string(),
      AttributeValue::S(holder_id.to_string()),
    );
    values.insert(
      ":session".to_string(),
      AttributeValue::S(lease_session_id.to_string()),
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
    let condition = format!("{ATTR_HOLDER} = :holder AND {ATTR_SESSION} = :session");
    let response = retry_dynamo_transaction_conflicts(
      "producer_lease_release",
      || {
        self
          .client
          .update_item()
          .table_name(&self.table_name)
          .key(ATTR_PK, AttributeValue::S(key.format()))
          .update_expression(update.clone())
          .condition_expression(condition.clone())
          .set_expression_attribute_values(Some(values.clone()))
          .return_values(ReturnValue::AllNew)
          .return_consumed_capacity(ReturnConsumedCapacity::Total)
          .send()
      },
      is_dynamo_transaction_conflict,
    )
    .await;

    match response {
      Ok(output) => {
        self.record_write_capacity(output.consumed_capacity.as_ref());
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
        if lease.lease_expiration_at <= now {
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
  lease_epoch: u64,
  lease_session_id: String,
  lease_expiration_ts_ms: i64,
  max_allocated_seq: Option<u64>,
}

impl DynamoLeaseItem {
  fn into_lease(self, key: ProducerPartitionLeaseKey) -> ProducerPartitionLease {
    ProducerPartitionLease {
      key,
      fence: ProducerLeaseFence {
        holder_id: self.holder_id,
        lease_epoch: self.lease_epoch,
        lease_session_id: self.lease_session_id,
      },
      lease_expiration_at: offset_datetime_from_unix_millis(self.lease_expiration_ts_ms),
      max_allocated_seq: self.max_allocated_seq,
    }
  }
}

fn reservation_from_old(old_max: Option<u64>, reservation_size: u64) -> Result<SeqRange> {
  let start = old_max.map_or(Ok(0), |max| {
    max
      .checked_add(1)
      .ok_or_else(|| anyhow!("sequence range overflow"))
  })?;

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

fn parse_u64(value: &AttributeValue) -> Result<u64> {
  match value {
    AttributeValue::N(value) => value
      .parse::<u64>()
      .map_err(|error| anyhow!("invalid number {value}: {error}")),
    other => Err(anyhow!("unexpected attribute value {other:?}")),
  }
}

fn reservation_would_overflow(
  lease: &ProducerPartitionLease,
  holder_id: &str,
  lease_session_id: &str,
  now: OffsetDateTime,
  reservation_size: Option<u64>,
) -> bool {
  reservation_size.is_some_and(|size| {
    lease.fence.holder_id == holder_id
      && lease.fence.lease_session_id == lease_session_id
      && lease.lease_expiration_at > now
      && lease
        .max_allocated_seq
        .is_some_and(|max_allocated_seq| max_allocated_seq > u64::MAX - size)
  })
}

fn expires_at(now: OffsetDateTime, lease_duration: Duration) -> Result<OffsetDateTime> {
  now
    .checked_add(lease_duration)
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
