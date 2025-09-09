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

    assert_eq!(active_count, expected_active);
    assert_eq!(failing_count, expected_failing);
    assert_eq!(paused_count, expected_paused);
    assert_eq!(stopped_count, expected_stopped);
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
