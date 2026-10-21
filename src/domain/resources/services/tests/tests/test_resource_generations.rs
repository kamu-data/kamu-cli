// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;

use kamu_resources::{ResourcePhase, ResourceStatusConflictError};
use pretty_assertions::assert_eq;

use crate::tests::utils::{ResourceLifecycleHarness, TestResourceLifecycleError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_new_resource_starts_both_generations_at_one() {
    let harness = ResourceLifecycleHarness::new();

    assert_eq!(harness.generations(), (1, 1));
    assert_eq!(harness.phase(), ResourcePhase::Pending);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_status_updates_bump_status_generation_and_updated_at() {
    let mut harness = ResourceLifecycleHarness::new();

    harness.mark_started();
    assert_eq!(harness.generations(), (1, 2));
    assert_eq!(harness.phase(), ResourcePhase::Reconciling);
    assert_eq!(harness.updated_at(), harness.now);

    harness.mark_succeeded();
    assert_eq!(harness.generations(), (1, 3));
    assert_eq!(harness.phase(), ResourcePhase::Ready);
    assert_eq!(harness.updated_at(), harness.now);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_spec_update_writes_no_status() {
    let mut harness = ResourceLifecycleHarness::reconciled();
    let status_before = harness.status();

    harness.apply(None, "v2");

    assert_eq!(harness.generations(), (2, 3));
    assert_eq!(harness.phase(), ResourcePhase::Pending);
    assert_eq!(
        harness.status().conditions,
        status_before.conditions,
        "conditions stay until the controller replaces them"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_headers_update_bumps_generation() {
    let mut harness = ResourceLifecycleHarness::reconciled();

    harness.apply(Some("prod"), "v1");

    assert_eq!(harness.generations(), (2, 3));
    assert_eq!(harness.phase(), ResourcePhase::Pending);
    assert!(harness.needs_reconciliation());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_headers_and_spec_update_in_one_apply_bump_generation_once() {
    let mut harness = ResourceLifecycleHarness::reconciled();

    harness.apply(Some("prod"), "v2");

    assert_eq!(harness.generations(), (2, 3));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_unchanged_apply_bumps_nothing() {
    let mut harness = ResourceLifecycleHarness::reconciled();

    harness.apply(None, "v1");

    assert_eq!(harness.generations(), (1, 3));
    assert_eq!(harness.phase(), ResourcePhase::Ready);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_stored_headers_update_without_generation_keeps_it() {
    let mut harness = ResourceLifecycleHarness::reconciled();

    harness.apply_stored_headers_update_without_generation(Some("prod"));

    assert_eq!(harness.generations(), (1, 3));
    assert_eq!(harness.phase(), ResourcePhase::Ready);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_stale_status_generation_is_a_conflict() {
    let mut harness = ResourceLifecycleHarness::new();
    harness.mark_started();
    let status_before = harness.status();

    let result = harness.update_status(1, ResourceLifecycleHarness::ready_status());

    assert_matches!(
        result,
        Err(TestResourceLifecycleError::StatusConflict(
            ResourceStatusConflictError {
                expected: 1,
                actual: 2
            }
        ))
    );
    assert_eq!(harness.status(), status_before);
    assert_eq!(harness.generations(), (1, 2));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_unchanged_status_is_not_an_update() {
    let mut harness = ResourceLifecycleHarness::reconciled();
    let updated_at_before = harness.updated_at();

    harness.update_status(3, harness.status()).unwrap();

    assert_eq!(harness.generations(), (1, 3));
    assert_eq!(harness.updated_at(), updated_at_before);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_failed_then_retried_then_degraded() {
    let mut harness = ResourceLifecycleHarness::new();

    harness.mark_started();
    harness.mark_failed();
    assert_eq!(harness.phase(), ResourcePhase::Failed);
    assert!(harness.needs_reconciliation());
    harness.assert_reconciliation_error(Some("internal-error"));

    // The error of the previous attempt stays visible during a retry
    harness.mark_started();
    assert_eq!(harness.phase(), ResourcePhase::Reconciling);
    harness.assert_reconciliation_error(Some("internal-error"));

    harness.mark_succeeded();
    assert_eq!(harness.phase(), ResourcePhase::Ready);
    assert_eq!(harness.generations(), (1, 5));
    harness.assert_reconciliation_error(None);

    harness.apply(None, "v2");
    harness.mark_started();
    harness.mark_failed();
    assert_eq!(harness.phase(), ResourcePhase::Degraded);
    assert_eq!(harness.generations(), (2, 7));
    harness.assert_reconciliation_error(Some("internal-error"));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_outcome_for_superseded_generation_is_skipped() {
    let mut harness = ResourceLifecycleHarness::new();
    harness.mark_started();

    harness.apply(None, "v2");
    harness.mark_succeeded_for_generation(1);

    assert_eq!(harness.generations(), (2, 2));
    assert_eq!(harness.phase(), ResourcePhase::Pending);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_success_of_unobserved_generation_is_an_invariant_violation() {
    let mut harness = ResourceLifecycleHarness::reconciled();
    harness.apply(None, "v2");
    let generations = harness.generations();

    assert_matches!(
        harness.try_mark_succeeded(),
        Err(TestResourceLifecycleError::InvariantViolation(_))
    );
    assert_eq!(harness.generations(), generations);
    assert_eq!(harness.phase(), ResourcePhase::Pending);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_observing_an_older_generation_is_an_invariant_violation() {
    let mut harness = ResourceLifecycleHarness::new();
    let mut status = harness.status();
    status.observed_generation = Some(5);
    let status_generation = harness.generations().1;
    harness.update_status(status_generation, status).unwrap();
    let generations = harness.generations();

    assert_matches!(
        harness.try_mark_started(),
        Err(TestResourceLifecycleError::InvariantViolation(_))
    );
    assert_eq!(harness.generations(), generations);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
