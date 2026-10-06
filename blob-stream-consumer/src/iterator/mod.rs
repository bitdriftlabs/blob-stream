#[cfg(test)]
#[path = "./iterator_test.rs"]
mod tests;

mod api;
mod builder;
mod delivery;
mod driver;
mod facade;
mod prefetch;
mod shared;

pub use api::{
  AssignmentCallback,
  ConsumerCoordinationSource,
  ConsumerIterator,
  ConsumerLifecycleHooks,
  ConsumerRecord,
  ConsumerSeekTarget,
  CoordinationSnapshot,
  NextBatchResult,
  NextResult,
  NoopConsumerLifecycleHooks,
  RevokedPartitions,
};
pub use builder::{ConsumerIteratorBuilder, TopicPartitionLayout};
pub use facade::ConsumerIteratorImpl;
pub use shared::{ConsumerDeliveryState, ConsumerSharedState};
