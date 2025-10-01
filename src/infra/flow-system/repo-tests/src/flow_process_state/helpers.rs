// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_adapter_flow_dataset::{FLOW_TYPE_DATASET_INGEST, FLOW_TYPE_DATASET_TRANSFORM};
use kamu_adapter_flow_webhook::FLOW_TYPE_WEBHOOK_DELIVER;
use kamu_flow_system::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn assert_flow_type_distribution(
    processes: &[FlowProcessState],
    expected_ingest: usize,
    expected_transform: usize,
    expected_webhook: usize,
) {
    let ingest_count = processes
        .iter()
        .filter(|p| p.flow_binding().flow_type == FLOW_TYPE_DATASET_INGEST)
        .count();
    let transform_count = processes
        .iter()
        .filter(|p| p.flow_binding().flow_type == FLOW_TYPE_DATASET_TRANSFORM)
        .count();
    let webhook_count = processes
        .iter()
        .filter(|p| p.flow_binding().flow_type == FLOW_TYPE_WEBHOOK_DELIVER)
        .count();

    assert_eq!(ingest_count, expected_ingest);
    assert_eq!(transform_count, expected_transform);
    assert_eq!(webhook_count, expected_webhook);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn assert_effective_state_distribution(
    processes: &[FlowProcessState],
    expected_active: usize,
    expected_failing: usize,
    expected_paused: usize,
    expected_stopped: usize,
    expected_unconfigured: usize,
) {
    let active_count = processes
        .iter()
        .filter(|p| p.effective_state() == FlowProcessEffectiveState::Active)
        .count();
    let failing_count = processes
        .iter()
        .filter(|p| p.effective_state() == FlowProcessEffectiveState::Failing)
        .count();
    let paused_count = processes
        .iter()
        .filter(|p| p.effective_state() == FlowProcessEffectiveState::PausedManual)
        .count();
    let stopped_count = processes
        .iter()
        .filter(|p| p.effective_state() == FlowProcessEffectiveState::StoppedAuto)
        .count();
    let unconfigured_count = processes
        .iter()
        .filter(|p| p.effective_state() == FlowProcessEffectiveState::Unconfigured)
        .count();

    assert_eq!(active_count, expected_active);
    assert_eq!(failing_count, expected_failing);
    assert_eq!(paused_count, expected_paused);
    assert_eq!(stopped_count, expected_stopped);
    assert_eq!(unconfigured_count, expected_unconfigured);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn assert_last_attempt_at_ordering(
    processes: &[FlowProcessState],
    desc: bool,
    context: &str,
) {
    // Check NULLS LAST behavior: non-null values should come before null values
    let mut found_null = false;
    for process in processes {
        match process.last_attempt_at() {
            Some(_) => {
                // If we already found a null, this violates NULLS LAST
                assert!(
                    !found_null,
                    "Found non-null last_attempt_at after null value when ordering {} in {}",
                    if desc { "DESC" } else { "ASC" },
                    context
                );
            }
            None => {
                found_null = true;
            }
        }
    }

    // Check ordering among non-null values
    let non_null_dates: Vec<_> = processes
        .iter()
        .filter_map(FlowProcessState::last_attempt_at)
        .collect();

    for i in 1..non_null_dates.len() {
        if desc {
            // DESC: newer dates should come first (dates[i-1] >= dates[i])
            assert!(
                non_null_dates[i - 1] >= non_null_dates[i],
                "Dates not in descending order in {}: {:?} should be >= {:?}",
                context,
                non_null_dates[i - 1],
                non_null_dates[i]
            );
        } else {
            // ASC: older dates should come first (dates[i-1] <= dates[i])
            assert!(
                non_null_dates[i - 1] <= non_null_dates[i],
                "Dates not in ascending order in {}: {:?} should be <= {:?}",
                context,
                non_null_dates[i - 1],
                non_null_dates[i]
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn assert_next_planned_at_ordering(
    processes: &[FlowProcessState],
    desc: bool,
    context: &str,
) {
    // Check NULLS LAST behavior: non-null values should come before null values
    let mut found_null = false;
    for process in processes {
        match process.next_planned_at() {
            Some(_) => {
                // If we already found a null, this violates NULLS LAST
                assert!(
                    !found_null,
                    "Found non-null next_planned_at after null value when ordering {} in {}",
                    if desc { "DESC" } else { "ASC" },
                    context
                );
            }
            None => {
                found_null = true;
            }
        }
    }

    // Check ordering among non-null values
    let non_null_dates: Vec<_> = processes
        .iter()
        .filter_map(FlowProcessState::next_planned_at)
        .collect();

    for i in 1..non_null_dates.len() {
        if desc {
            // DESC: future dates should come first (dates[i-1] >= dates[i])
            assert!(
                non_null_dates[i - 1] >= non_null_dates[i],
                "Dates not in descending order in {}: {:?} should be >= {:?}",
                context,
                non_null_dates[i - 1],
                non_null_dates[i]
            );
        } else {
            // ASC: sooner dates should come first (dates[i-1] <= dates[i])
            assert!(
                non_null_dates[i - 1] <= non_null_dates[i],
                "Dates not in ascending order in {}: {:?} should be <= {:?}",
                context,
                non_null_dates[i - 1],
                non_null_dates[i]
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn assert_last_failure_at_ordering(
    processes: &[FlowProcessState],
    desc: bool,
    context: &str,
) {
    // Check NULLS LAST behavior: non-null values should come before null values
    let mut found_null = false;
    for process in processes {
        match process.last_failure_at() {
            Some(_) => {
                // If we already found a null, this violates NULLS LAST
                assert!(
                    !found_null,
                    "Found non-null last_failure_at after null value when ordering {} in {}",
                    if desc { "DESC" } else { "ASC" },
                    context
                );
            }
            None => {
                found_null = true;
            }
        }
    }

    // Check ordering among non-null values
    let non_null_dates: Vec<_> = processes
        .iter()
        .filter_map(FlowProcessState::last_failure_at)
        .collect();

    for i in 1..non_null_dates.len() {
        if desc {
            // DESC: recent failures should come first (dates[i-1] >= dates[i])
            assert!(
                non_null_dates[i - 1] >= non_null_dates[i],
                "Dates not in descending order in {}: {:?} should be >= {:?}",
                context,
                non_null_dates[i - 1],
                non_null_dates[i]
            );
        } else {
            // ASC: older failures should come first (dates[i-1] <= dates[i])
            assert!(
                non_null_dates[i - 1] <= non_null_dates[i],
                "Dates not in ascending order in {}: {:?} should be <= {:?}",
                context,
                non_null_dates[i - 1],
                non_null_dates[i]
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn assert_consecutive_failures_ordering(
    processes: &[FlowProcessState],
    desc: bool,
    context: &str,
) {
    let failure_counts: Vec<_> = processes
        .iter()
        .map(FlowProcessState::consecutive_failures)
        .collect();

    for i in 1..failure_counts.len() {
        if desc {
            // DESC: higher failure counts should come first
            assert!(
                failure_counts[i - 1] >= failure_counts[i],
                "Consecutive failures not in descending order in {}: {} should be >= {}",
                context,
                failure_counts[i - 1],
                failure_counts[i]
            );
        } else {
            // ASC: lower failure counts should come first
            assert!(
                failure_counts[i - 1] <= failure_counts[i],
                "Consecutive failures not in ascending order in {}: {} should be <= {}",
                context,
                failure_counts[i - 1],
                failure_counts[i]
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn assert_effective_state_ordering(
    processes: &[FlowProcessState],
    desc: bool,
    context: &str,
) {
    let states: Vec<_> = processes
        .iter()
        .map(FlowProcessState::effective_state)
        .collect();

    for i in 1..states.len() {
        if desc {
            // DESC: higher enum values should come first (Active > PausedManual > Failing >
            // StoppedAuto)
            assert!(
                states[i - 1] >= states[i],
                "Effective states not in descending order in {}: {:?} should be >= {:?}",
                context,
                states[i - 1],
                states[i]
            );
        } else {
            // ASC: lower enum values should come first (StoppedAuto < Failing <
            // PausedManual < Active)
            assert!(
                states[i - 1] <= states[i],
                "Effective states not in ascending order in {}: {:?} should be <= {:?}",
                context,
                states[i - 1],
                states[i]
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn assert_flow_type_ordering(processes: &[FlowProcessState], desc: bool, context: &str) {
    let flow_types: Vec<_> = processes
        .iter()
        .map(|p| p.flow_binding().flow_type.as_str())
        .collect();

    for i in 1..flow_types.len() {
        if desc {
            // DESC: Z-A alphabetical order of flow types
            assert!(
                flow_types[i - 1] >= flow_types[i],
                "Flow types not in descending alphabetical order in {}: '{}' should be >= '{}'",
                context,
                flow_types[i - 1],
                flow_types[i]
            );
        } else {
            // ASC: A-Z alphabetical order of flow types
            assert!(
                flow_types[i - 1] <= flow_types[i],
                "Flow types not in ascending alphabetical order in {}: '{}' should be <= '{}'",
                context,
                flow_types[i - 1],
                flow_types[i]
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
