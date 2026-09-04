//! The controller starts at the effective flush-delay maximum and only affects future scheduler
//! cycles. While disabled, it holds that maximum. Reconfiguration resets the outcome history and
//! clamps the active delay to the new floor and maximum.
//!
//! A completed plan is classified as a split, a successful unsplit plan, or a failed unsplit
//! plan. Three consecutive split plans are required before reducing the delay, and three
//! consecutive successful unsplit plans are required before recovering it. The opposite outcome,
//! or a failed unsplit plan, clears both streaks. An adjustment clears its streak, so every
//! reduction or recovery step requires a new three-plan run of matching outcomes.
//!
//! A split plan with $s$ additional objects first derives an ideal delay of
//! `max(floor, ceil(current_delay / (s + 1)))`. To avoid abrupt reductions, it moves one quarter
//! of the distance toward that target, rounding the decrement up. A successful unsplit plan
//! recovers by half the remaining headroom, rounded up: `min(maximum, current_delay +
//! ceil((maximum - current_delay) / 2))`.

use super::config::AdaptiveFlushDelayConfig;
use time::Duration;

#[cfg(test)]
#[path = "./adaptive_flush_delay_test.rs"]
mod tests;

const CONSECUTIVE_OUTCOMES_REQUIRED_FOR_ADJUSTMENT: u64 = 3;
const SPLIT_REDUCTION_FRACTION_DENOMINATOR: i128 = 4;

//
// AdaptiveFlushDelay
//

/// Maintains the delay applied to future broker flush-scheduler cycles.
pub(super) struct AdaptiveFlushDelay {
  config: AdaptiveFlushDelayConfig,
  current_delay: Duration,
  consecutive_split_plans: u64,
  consecutive_successful_unsplit_plans: u64,
}

impl AdaptiveFlushDelay {
  pub(super) fn new(config: AdaptiveFlushDelayConfig) -> Self {
    Self {
      current_delay: config.max_delay,
      config,
      consecutive_split_plans: 0,
      consecutive_successful_unsplit_plans: 0,
    }
  }

  #[must_use]
  pub(super) fn current_delay(&self) -> Duration {
    self.current_delay
  }

  pub(super) fn reconfigure(&mut self, config: AdaptiveFlushDelayConfig) {
    let was_enabled = self.config.enabled;
    self.config = config;
    self.reset_streaks();
    if !config.enabled || !was_enabled {
      self.current_delay = config.max_delay;
      return;
    }

    self.current_delay = self.current_delay.clamp(config.floor, config.max_delay);
  }

  /// Records one terminal flush-plan outcome and returns whether it changed the future delay.
  pub(super) fn record_flush_result(&mut self, split_count: u64, successful: bool) -> bool {
    let previous_delay = self.current_delay;
    if !self.config.enabled {
      self.reset_streaks();
      self.current_delay = self.config.max_delay;
      return self.current_delay != previous_delay;
    }

    if split_count > 0 {
      self.consecutive_split_plans = self.consecutive_split_plans.saturating_add(1);
      self.consecutive_successful_unsplit_plans = 0;
      if self.consecutive_split_plans < CONSECUTIVE_OUTCOMES_REQUIRED_FOR_ADJUSTMENT {
        return false;
      }

      let divisor = i128::from(split_count.saturating_add(1));
      let current_milliseconds = self.duration_milliseconds();
      let target_milliseconds = divide_rounding_up(current_milliseconds, divisor)
        .max(self.floor_milliseconds())
        .min(self.maximum_milliseconds());
      let reduction = divide_rounding_up(
        current_milliseconds.saturating_sub(target_milliseconds),
        SPLIT_REDUCTION_FRACTION_DENOMINATOR,
      );
      self.current_delay = duration_from_milliseconds(
        current_milliseconds
          .saturating_sub(reduction)
          .max(self.floor_milliseconds()),
      );
      if self.current_delay != previous_delay {
        self.reset_streaks();
      }
    } else if successful {
      self.consecutive_successful_unsplit_plans =
        self.consecutive_successful_unsplit_plans.saturating_add(1);
      self.consecutive_split_plans = 0;
      if self.consecutive_successful_unsplit_plans < CONSECUTIVE_OUTCOMES_REQUIRED_FOR_ADJUSTMENT {
        return false;
      }

      let current_milliseconds = self.duration_milliseconds();
      let remaining = self
        .maximum_milliseconds()
        .saturating_sub(current_milliseconds);
      self.current_delay = duration_from_milliseconds(
        current_milliseconds
          .saturating_add(divide_rounding_up(remaining, 2))
          .min(self.maximum_milliseconds()),
      );
      if self.current_delay != previous_delay {
        self.reset_streaks();
      }
    } else {
      self.reset_streaks();
    }

    self.current_delay != previous_delay
  }

  fn reset_streaks(&mut self) {
    self.consecutive_split_plans = 0;
    self.consecutive_successful_unsplit_plans = 0;
  }

  fn duration_milliseconds(&self) -> i128 {
    self.current_delay.whole_milliseconds()
  }

  fn floor_milliseconds(&self) -> i128 {
    self.config.floor.whole_milliseconds()
  }

  fn maximum_milliseconds(&self) -> i128 {
    self.config.max_delay.whole_milliseconds()
  }
}

fn divide_rounding_up(dividend: i128, divisor: i128) -> i128 {
  if dividend == 0 {
    return 0;
  }
  dividend.saturating_sub(1) / divisor + 1
}

fn duration_from_milliseconds(milliseconds: i128) -> Duration {
  Duration::milliseconds(i64::try_from(milliseconds).unwrap_or(i64::MAX))
}
