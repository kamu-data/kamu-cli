// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use strum::{Display, EnumCount, EnumString};

use crate::FlowTriggerStopPolicy;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(
    Debug, Clone, Copy, Eq, PartialEq, Hash, Ord, PartialOrd, EnumCount, Display, EnumString,
)]
#[cfg_attr(
    feature = "sqlx",
    derive(sqlx::Type),
    sqlx(type_name = "flow_process_effective_state", rename_all = "snake_case")
)]
#[strum(serialize_all = "snake_case")]
pub enum FlowProcessEffectiveState {
    StoppedAuto,
    Failing,
    PausedManual,
    Active,
}

impl FlowProcessEffectiveState {
    pub fn calculate(
        paused_manually: bool,
        consecutive_failures: u32,
        stop_policy: FlowTriggerStopPolicy,
    ) -> Self {
        if paused_manually {
            FlowProcessEffectiveState::PausedManual
        } else if consecutive_failures > 0 {
            match stop_policy {
                FlowTriggerStopPolicy::AfterConsecutiveFailures { failures_count }
                    if consecutive_failures >= failures_count.into_inner() =>
                {
                    FlowProcessEffectiveState::StoppedAuto
                }
                FlowTriggerStopPolicy::AfterConsecutiveFailures { .. }
                | FlowTriggerStopPolicy::Never => FlowProcessEffectiveState::Failing,
            }
        } else {
            FlowProcessEffectiveState::Active
        }
    }

    /// Returns true if the flow process is in a running state (Active or
    /// Failing), false if it's stopped or paused
    pub fn is_running(self) -> bool {
        matches!(
            self,
            FlowProcessEffectiveState::Active | FlowProcessEffectiveState::Failing
        )
    }
}

impl<T> std::ops::Index<FlowProcessEffectiveState> for [T] {
    type Output = T;

    fn index(&self, state: FlowProcessEffectiveState) -> &Self::Output {
        &self[state as usize]
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ConsecutiveFailuresCount;

    #[test]
    fn test_calculate_active_state() {
        // No manual pause, no failures -> Active
        let state = FlowProcessEffectiveState::calculate(
            false, // not paused manually
            0,     // no consecutive failures
            FlowTriggerStopPolicy::Never,
        );
        assert_eq!(state, FlowProcessEffectiveState::Active);

        // No manual pause, no failures, with stop policy -> Active
        let state = FlowProcessEffectiveState::calculate(
            false, // not paused manually
            0,     // no consecutive failures
            FlowTriggerStopPolicy::AfterConsecutiveFailures {
                failures_count: ConsecutiveFailuresCount::try_new(3).unwrap(),
            },
        );
        assert_eq!(state, FlowProcessEffectiveState::Active);
    }

    #[test]
    fn test_is_running() {
        // Active and Failing states should be considered running
        assert!(FlowProcessEffectiveState::Active.is_running());
        assert!(FlowProcessEffectiveState::Failing.is_running());

        // StoppedAuto and PausedManual should not be considered running
        assert!(!FlowProcessEffectiveState::StoppedAuto.is_running());
        assert!(!FlowProcessEffectiveState::PausedManual.is_running());
    }

    #[test]
    fn test_calculate_paused_manual() {
        // Manual pause takes precedence over everything else
        let state = FlowProcessEffectiveState::calculate(
            true, // paused manually
            0,    // no consecutive failures
            FlowTriggerStopPolicy::Never,
        );
        assert_eq!(state, FlowProcessEffectiveState::PausedManual);

        // Manual pause with failures and stop policy
        let state = FlowProcessEffectiveState::calculate(
            true, // paused manually
            5,    // has consecutive failures
            FlowTriggerStopPolicy::AfterConsecutiveFailures {
                failures_count: ConsecutiveFailuresCount::try_new(3).unwrap(),
            },
        );
        assert_eq!(state, FlowProcessEffectiveState::PausedManual);
    }

    #[test]
    fn test_calculate_failing_with_never_policy() {
        // Has failures but policy is Never -> Failing
        let state = FlowProcessEffectiveState::calculate(
            false, // not paused manually
            2,     // has consecutive failures
            FlowTriggerStopPolicy::Never,
        );
        assert_eq!(state, FlowProcessEffectiveState::Failing);

        // Many failures but policy is Never -> still Failing
        let state = FlowProcessEffectiveState::calculate(
            false, // not paused manually
            10,    // many consecutive failures
            FlowTriggerStopPolicy::Never,
        );
        assert_eq!(state, FlowProcessEffectiveState::Failing);
    }

    #[test]
    fn test_calculate_failing_below_threshold() {
        // Has failures but below threshold -> Failing
        let state = FlowProcessEffectiveState::calculate(
            false, // not paused manually
            2,     // consecutive failures below threshold
            FlowTriggerStopPolicy::AfterConsecutiveFailures {
                failures_count: ConsecutiveFailuresCount::try_new(3).unwrap(),
            },
        );
        assert_eq!(state, FlowProcessEffectiveState::Failing);

        // Exactly one below threshold -> Failing
        let state = FlowProcessEffectiveState::calculate(
            false, // not paused manually
            4,     // one below threshold
            FlowTriggerStopPolicy::AfterConsecutiveFailures {
                failures_count: ConsecutiveFailuresCount::try_new(5).unwrap(),
            },
        );
        assert_eq!(state, FlowProcessEffectiveState::Failing);
    }

    #[test]
    fn test_calculate_stopped_auto_at_threshold() {
        // Failures exactly at threshold -> StoppedAuto
        let state = FlowProcessEffectiveState::calculate(
            false, // not paused manually
            3,     // consecutive failures at threshold
            FlowTriggerStopPolicy::AfterConsecutiveFailures {
                failures_count: ConsecutiveFailuresCount::try_new(3).unwrap(),
            },
        );
        assert_eq!(state, FlowProcessEffectiveState::StoppedAuto);
    }

    #[test]
    fn test_calculate_stopped_auto_above_threshold() {
        // Failures above threshold -> StoppedAuto
        let state = FlowProcessEffectiveState::calculate(
            false, // not paused manually
            5,     // consecutive failures above threshold
            FlowTriggerStopPolicy::AfterConsecutiveFailures {
                failures_count: ConsecutiveFailuresCount::try_new(3).unwrap(),
            },
        );
        assert_eq!(state, FlowProcessEffectiveState::StoppedAuto);

        // Way above threshold -> StoppedAuto
        let state = FlowProcessEffectiveState::calculate(
            false, // not paused manually
            10,    // many consecutive failures above threshold
            FlowTriggerStopPolicy::AfterConsecutiveFailures {
                failures_count: ConsecutiveFailuresCount::try_new(2).unwrap(),
            },
        );
        assert_eq!(state, FlowProcessEffectiveState::StoppedAuto);
    }

    #[test]
    fn test_calculate_edge_cases() {
        // Test with minimum valid ConsecutiveFailuresCount (1)
        let state = FlowProcessEffectiveState::calculate(
            false, // not paused manually
            1,     // one failure at threshold
            FlowTriggerStopPolicy::AfterConsecutiveFailures {
                failures_count: ConsecutiveFailuresCount::try_new(1).unwrap(),
            },
        );
        assert_eq!(state, FlowProcessEffectiveState::StoppedAuto);

        // Test with maximum valid ConsecutiveFailuresCount (10)
        let state = FlowProcessEffectiveState::calculate(
            false, // not paused manually
            10,    // ten failures at threshold
            FlowTriggerStopPolicy::AfterConsecutiveFailures {
                failures_count: ConsecutiveFailuresCount::try_new(10).unwrap(),
            },
        );
        assert_eq!(state, FlowProcessEffectiveState::StoppedAuto);

        // Test just below maximum threshold
        let state = FlowProcessEffectiveState::calculate(
            false, // not paused manually
            9,     // nine failures below threshold
            FlowTriggerStopPolicy::AfterConsecutiveFailures {
                failures_count: ConsecutiveFailuresCount::try_new(10).unwrap(),
            },
        );
        assert_eq!(state, FlowProcessEffectiveState::Failing);
    }

    #[test]
    fn test_calculate_priority_precedence() {
        // Manual pause should take precedence over auto-stop
        let state = FlowProcessEffectiveState::calculate(
            true, // paused manually (highest priority)
            5,    // failures above threshold
            FlowTriggerStopPolicy::AfterConsecutiveFailures {
                failures_count: ConsecutiveFailuresCount::try_new(3).unwrap(),
            },
        );
        assert_eq!(state, FlowProcessEffectiveState::PausedManual);

        // When not manually paused, auto-stop should take precedence over failing
        let state = FlowProcessEffectiveState::calculate(
            false, // not paused manually
            5,     // failures above threshold
            FlowTriggerStopPolicy::AfterConsecutiveFailures {
                failures_count: ConsecutiveFailuresCount::try_new(3).unwrap(),
            },
        );
        assert_eq!(state, FlowProcessEffectiveState::StoppedAuto);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
