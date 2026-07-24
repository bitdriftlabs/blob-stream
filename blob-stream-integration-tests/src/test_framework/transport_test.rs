use super::{ReorderCoordinator, ReorderCoordinatorState, ReorderOutcome};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

#[tokio::test]
async fn reorder_rendezvous_releases_second_request_before_first() {
  let coordinator = Arc::new(ReorderCoordinator {
    state: Mutex::new(ReorderCoordinatorState::default()),
  });

  let (first, second) = tokio::join!(
    coordinator.rendezvous("broker-a", Duration::from_secs(1)),
    coordinator.rendezvous("broker-a", Duration::from_secs(1)),
  );

  assert_eq!(first, ReorderOutcome::FirstReleased);
  assert_eq!(second, ReorderOutcome::SecondReleased);
}
