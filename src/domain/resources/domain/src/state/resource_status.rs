// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};

use crate::{
    RECONCILIATION_ERROR_CODE_CLEANUP_FAILED,
    ReconciliationError,
    ResourceConditionsReconciliationErrorExt,
    ResourceHeaders,
    ResourcePhase,
    empty_resource_conditions,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type ResourceStatus = odf::metadata::resources::ResourceStatus;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn new_pending_resource_status() -> ResourceStatus {
    ResourceStatus {
        phase: ResourcePhase::Pending,
        observed_generation: None,
        observed_at: None,
        reconciled_generation: None,
        reconciled_at: None,
        conditions: empty_resource_conditions(),
    }
}

pub fn resource_status_from_json(value: &serde_json::Value) -> Option<ResourceStatus> {
    let proxy: odf::metadata::serde::yaml::resources::ResourceStatus =
        serde_json::from_value(value.clone()).ok()?;
    proxy.try_into().ok()
}

pub fn resource_status_to_json(status: &ResourceStatus) -> serde_json::Value {
    let proxy: odf::metadata::serde::yaml::resources::ResourceStatus = status.clone().into();
    serde_json::to_value(proxy).unwrap()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait ResourceStatusExt {
    fn was_observed(&self, generation: u64) -> bool;

    fn was_reconciled(&self, generation: u64) -> bool;

    /// Derives the phase from the headers: deletion phases take precedence,
    /// then `Pending` while the main controller has not observed the current
    /// generation. Runs after every change of the resource.
    fn align_phase(&mut self, headers: &ResourceHeaders);

    /// Updates observed generation and indicates that reconciliation had started.
    /// The error of a previous attempt stays until this one has a result.
    fn mark_reconciling(&mut self, observed_generation: u64, now: DateTime<Utc>);

    /// Updates reconciled generation and clears the reconciliation error
    fn mark_ready(&mut self, reconciled_generation: u64, now: DateTime<Utc>);

    /// Indicates failed reconciliation attempt
    fn mark_failed_or_degraded(&mut self, code: impl Into<String>, message: impl Into<String>);

    /// Indicates that cleaning up a resource being deleted failed
    fn mark_cleanup_failed(&mut self, message: impl Into<String>);
}

impl ResourceStatusExt for ResourceStatus {
    fn was_observed(&self, generation: u64) -> bool {
        self.observed_generation
            .is_some_and(|observed_generation| observed_generation >= generation)
    }

    fn was_reconciled(&self, generation: u64) -> bool {
        self.reconciled_generation
            .is_some_and(|reconciled_generation| reconciled_generation >= generation)
    }

    fn align_phase(&mut self, headers: &ResourceHeaders) {
        if headers.deleted_at.is_some() {
            self.phase = ResourcePhase::Deleted;
        } else if headers.deletion_requested_at.is_some() {
            self.phase = ResourcePhase::Deleting;
        } else if !self.was_observed(headers.generation) {
            self.phase = ResourcePhase::Pending;
        }
    }

    fn mark_reconciling(&mut self, observed_generation: u64, now: DateTime<Utc>) {
        // Equal when retrying a generation that failed to reconcile
        assert!(
            self.observed_generation.is_none()
                || self.observed_generation.unwrap() <= observed_generation,
            "Previous observed generation {:?} > {observed_generation}",
            self.observed_generation,
        );
        self.phase = ResourcePhase::Reconciling;
        self.observed_generation = Some(observed_generation);
        self.observed_at = Some(now);
    }

    fn mark_ready(&mut self, reconciled_generation: u64, now: DateTime<Utc>) {
        assert!(
            self.observed_generation.is_some()
                && self.observed_generation.unwrap() >= reconciled_generation,
            "Observed generation {:?} < {reconciled_generation}",
            self.observed_generation,
        );
        assert!(
            self.reconciled_generation.is_none()
                || self.reconciled_generation.unwrap() < reconciled_generation,
            "Previous reconciled generation {:?} >= {reconciled_generation}",
            self.reconciled_generation,
        );

        self.phase = ResourcePhase::Ready;
        self.reconciled_generation = Some(reconciled_generation);
        self.reconciled_at = Some(now);
        self.conditions.clear_reconciliation_error();
    }

    fn mark_failed_or_degraded(&mut self, code: impl Into<String>, message: impl Into<String>) {
        self.phase = if self.reconciled_generation.is_none() {
            ResourcePhase::Failed
        } else {
            ResourcePhase::Degraded
        };

        self.conditions
            .set_reconciliation_error(ReconciliationError {
                code: code.into(),
                message: message.into(),
            });
    }

    fn mark_cleanup_failed(&mut self, message: impl Into<String>) {
        self.conditions
            .set_reconciliation_error(ReconciliationError {
                code: RECONCILIATION_ERROR_CODE_CLEANUP_FAILED.to_string(),
                message: message.into(),
            });
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
