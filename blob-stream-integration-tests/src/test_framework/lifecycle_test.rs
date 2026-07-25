use super::{LifecycleEvent, TestLifecycleHooks};

#[tokio::test]
async fn duplicate_arm_preserves_original_gate() {
  let hooks = TestLifecycleHooks::default();
  let mut gate = hooks
    .arm(LifecycleEvent::ConsumerBeforeCommit)
    .await
    .unwrap();

  assert!(
    hooks
      .arm(LifecycleEvent::ConsumerBeforeCommit)
      .await
      .is_err()
  );

  let reach = tokio::spawn({
    let hooks = hooks.clone();
    async move {
      hooks
        .reach_consumer(LifecycleEvent::ConsumerBeforeCommit, "member-a", 1, &[])
        .await;
    }
  });
  gate.wait_until_reached().await.unwrap();
  gate.release().unwrap();
  reach.await.unwrap();
}

#[tokio::test]
async fn consumer_gate_prefers_exact_selector_over_wildcard() {
  let hooks = TestLifecycleHooks::default();
  let mut wildcard = hooks
    .arm_consumer(
      LifecycleEvent::ConsumerRebalanceApplied,
      "member-a",
      None,
      None,
    )
    .await
    .unwrap();
  let mut exact = hooks
    .arm_consumer(
      LifecycleEvent::ConsumerRebalanceApplied,
      "member-a",
      Some(3),
      Some(7),
    )
    .await
    .unwrap();

  let exact_reach = tokio::spawn({
    let hooks = hooks.clone();
    async move {
      hooks
        .reach_consumer(
          LifecycleEvent::ConsumerRebalanceApplied,
          "member-a",
          7,
          &[3],
        )
        .await;
    }
  });
  exact.wait_until_reached().await.unwrap();
  exact.release().unwrap();
  exact_reach.await.unwrap();

  let wildcard_reach = tokio::spawn({
    let hooks = hooks.clone();
    async move {
      hooks
        .reach_consumer(
          LifecycleEvent::ConsumerRebalanceApplied,
          "member-a",
          7,
          &[3],
        )
        .await;
    }
  });
  wildcard.wait_until_reached().await.unwrap();
  wildcard.release().unwrap();
  wildcard_reach.await.unwrap();
}
