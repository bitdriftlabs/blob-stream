use super::AdaptiveFlushDelay;
use crate::write::config::AdaptiveFlushDelayConfig;
use time::Duration;

fn config(
  enabled: bool,
  floor_milliseconds: i64,
  maximum_milliseconds: i64,
) -> AdaptiveFlushDelayConfig {
  AdaptiveFlushDelayConfig {
    enabled,
    floor: Duration::milliseconds(floor_milliseconds),
    max_delay: Duration::milliseconds(maximum_milliseconds),
  }
}

#[test]
fn disabled_controller_holds_the_effective_maximum() {
  let mut controller = AdaptiveFlushDelay::new(config(false, 100, 1_000));

  assert_eq!(controller.current_delay(), Duration::milliseconds(1_000));
  assert!(!controller.record_flush_result(3, true));
  assert_eq!(controller.current_delay(), Duration::milliseconds(1_000));
}

#[test]
fn splits_require_a_consecutive_streak_before_reducing_proportionally() {
  let mut controller = AdaptiveFlushDelay::new(config(true, 100, 1_000));

  assert!(!controller.record_flush_result(1, true));
  assert!(!controller.record_flush_result(1, true));
  assert!(!controller.record_flush_result(0, true));
  assert!(!controller.record_flush_result(1, true));
  assert!(!controller.record_flush_result(1, true));
  assert!(controller.record_flush_result(1, true));
  assert_eq!(controller.current_delay(), Duration::milliseconds(875));
  assert!(!controller.record_flush_result(4, false));
  assert!(!controller.record_flush_result(4, false));
  assert!(controller.record_flush_result(4, false));
  assert_eq!(controller.current_delay(), Duration::milliseconds(700));
  assert!(!controller.record_flush_result(1, true));
  assert_eq!(controller.current_delay(), Duration::milliseconds(700));
}

#[test]
fn successful_unsplit_plans_recover_half_the_remaining_headroom() {
  let mut controller = AdaptiveFlushDelay::new(config(true, 100, 1_000));
  for _ in 0 .. 3 {
    controller.record_flush_result(3, true);
  }

  assert_eq!(controller.current_delay(), Duration::milliseconds(812));
  assert!(!controller.record_flush_result(0, true));
  assert!(!controller.record_flush_result(0, true));
  assert!(controller.record_flush_result(0, true));
  assert_eq!(controller.current_delay(), Duration::milliseconds(906));
  assert!(!controller.record_flush_result(0, true));
  assert!(!controller.record_flush_result(0, true));
  assert!(controller.record_flush_result(0, true));
  assert_eq!(controller.current_delay(), Duration::milliseconds(953));
}

#[test]
fn failed_unsplit_plan_resets_the_recovery_streak() {
  let mut controller = AdaptiveFlushDelay::new(config(true, 100, 1_000));
  for _ in 0 .. 3 {
    controller.record_flush_result(1, true);
  }

  assert!(!controller.record_flush_result(0, true));
  assert!(!controller.record_flush_result(0, true));
  assert!(!controller.record_flush_result(0, false));
  assert!(!controller.record_flush_result(0, true));
  assert!(!controller.record_flush_result(0, true));
  assert!(controller.record_flush_result(0, true));
  assert_eq!(controller.current_delay(), Duration::milliseconds(938));
}

#[test]
fn failed_unsplit_plan_does_not_recover_the_delay() {
  let mut controller = AdaptiveFlushDelay::new(config(true, 100, 1_000));
  for _ in 0 .. 3 {
    controller.record_flush_result(1, true);
  }

  assert!(!controller.record_flush_result(0, false));
  assert_eq!(controller.current_delay(), Duration::milliseconds(875));
}

#[test]
fn millisecond_rounding_makes_progress_and_respects_bounds() {
  let mut controller = AdaptiveFlushDelay::new(config(true, 1, 3));
  for _ in 0 .. 3 {
    controller.record_flush_result(2, true);
  }

  assert_eq!(controller.current_delay(), Duration::milliseconds(2));
  for _ in 0 .. 3 {
    controller.record_flush_result(2, true);
  }
  assert_eq!(controller.current_delay(), Duration::milliseconds(1));
  controller.record_flush_result(0, true);
  controller.record_flush_result(0, true);
  assert!(controller.record_flush_result(0, true));
  assert_eq!(controller.current_delay(), Duration::milliseconds(2));
  controller.record_flush_result(0, true);
  controller.record_flush_result(0, true);
  assert!(controller.record_flush_result(0, true));
  assert_eq!(controller.current_delay(), Duration::milliseconds(3));
  controller.record_flush_result(0, true);
  controller.record_flush_result(0, true);
  assert!(!controller.record_flush_result(0, true));
}

#[test]
fn reconfiguration_resets_when_enablement_changes_and_clamps_otherwise() {
  let mut controller = AdaptiveFlushDelay::new(config(true, 100, 1_000));
  for _ in 0 .. 3 {
    controller.record_flush_result(1, true);
  }

  controller.reconfigure(config(true, 600, 800));
  assert_eq!(controller.current_delay(), Duration::milliseconds(800));
  controller.reconfigure(config(false, 100, 800));
  assert_eq!(controller.current_delay(), Duration::milliseconds(800));
  controller.reconfigure(config(true, 100, 800));
  assert_eq!(controller.current_delay(), Duration::milliseconds(800));
}

#[test]
fn reconfiguration_clears_an_incomplete_outcome_streak() {
  let mut controller = AdaptiveFlushDelay::new(config(true, 100, 1_000));
  controller.record_flush_result(1, true);
  controller.record_flush_result(1, true);

  controller.reconfigure(config(true, 100, 1_000));
  assert!(!controller.record_flush_result(1, true));
  assert!(!controller.record_flush_result(1, true));
  assert!(controller.record_flush_result(1, true));
  assert_eq!(controller.current_delay(), Duration::milliseconds(875));
}
