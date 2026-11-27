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
    ResourceConditions,
    ResourceConditionsReconciliationErrorExt,
    ResourceHeaders,
    ResourcePhase,
    empty_resource_conditions,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type ResourceStatus = odf::metadata::resources::ResourceStatus;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn new_pending_resource_status() -> ResourceStatus {
    new_pending_resource_status_with(empty_resource_conditions())
}

pub(crate) fn new_pending_resource_status_with(conditions: ResourceConditions) -> ResourceStatus {
    ResourceStatus {
        phase: ResourcePhase::Pending,
        observed_generation: None,
        observed_at: None,
        reconciled_generation: None,
        reconciled_at: None,
        conditions,
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

    /// The phase the headers and the rest of the status imply. Deletion phases
    /// take precedence, then `Pending` while the main controller has not
    /// observed the current generation; an observed generation is `Ready` once
    /// reconciled, `Failed` or `Degraded` while a `ReconciliationError` is
    /// present (`Degraded` if an earlier generation was reconciled), and
    /// `Reconciling` otherwise.
    fn derive_phase(&self, headers: &ResourceHeaders) -> ResourcePhase;

    /// Sets the phase to [`Self::derive_phase`]. Runs after every change of the
    /// resource and before every status update is saved, so no status writer
    /// sets the phase itself.
    fn align_phase(&mut self, headers: &ResourceHeaders);

    /// Updates observed generation and indicates that reconciliation had
    /// started, removing the error of a previous attempt.
    /// Fails, leaving the status as is, if the generation moves backwards.
    fn mark_reconciling(
        &mut self,
        observed_generation: u64,
        now: DateTime<Utc>,
    ) -> Result<(), IllegalStatusTransitionError>;

    /// Updates reconciled generation and clears the reconciliation error.
    /// Fails, leaving the status as is, if the generation was not observed or
    /// was already reconciled.
    fn mark_ready(
        &mut self,
        reconciled_generation: u64,
        now: DateTime<Utc>,
    ) -> Result<(), IllegalStatusTransitionError>;

    /// Records the error of a failed reconciliation attempt, which makes the
    /// phase `Failed` or `Degraded`
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

    fn derive_phase(&self, headers: &ResourceHeaders) -> ResourcePhase {
        if headers.deleted_at.is_some() {
            ResourcePhase::Deleted
        } else if headers.deletion_requested_at.is_some() {
            ResourcePhase::Deleting
        } else if !self.was_observed(headers.generation) {
            ResourcePhase::Pending
        } else if self.was_reconciled(headers.generation) {
            ResourcePhase::Ready
        } else if !self.conditions.has_reconciliation_error() {
            ResourcePhase::Reconciling
        } else if self.reconciled_generation.is_none() {
            ResourcePhase::Failed
        } else {
            ResourcePhase::Degraded
        }
    }

    fn align_phase(&mut self, headers: &ResourceHeaders) {
        self.phase = self.derive_phase(headers);
    }

    fn mark_reconciling(
        &mut self,
        observed_generation: u64,
        now: DateTime<Utc>,
    ) -> Result<(), IllegalStatusTransitionError> {
        // Equal when retrying a generation that failed to reconcile
        if let Some(previous) = self.observed_generation
            && previous > observed_generation
        {
            return Err(IllegalStatusTransitionError::new(format!(
                "Previous observed generation {previous} > {observed_generation}"
            )));
        }

        self.observed_generation = Some(observed_generation);
        self.observed_at = Some(now);
        self.conditions.clear_reconciliation_error();
        Ok(())
    }

    fn mark_ready(
        &mut self,
        reconciled_generation: u64,
        now: DateTime<Utc>,
    ) -> Result<(), IllegalStatusTransitionError> {
        if !self.was_observed(reconciled_generation) {
            return Err(IllegalStatusTransitionError::new(format!(
                "Observed generation {:?} < {reconciled_generation}",
                self.observed_generation,
            )));
        }
        if self.was_reconciled(reconciled_generation) {
            return Err(IllegalStatusTransitionError::new(format!(
                "Previous reconciled generation {:?} >= {reconciled_generation}",
                self.reconciled_generation,
            )));
        }

        self.reconciled_generation = Some(reconciled_generation);
        self.reconciled_at = Some(now);
        self.conditions.clear_reconciliation_error();
        Ok(())
    }

    fn mark_failed_or_degraded(&mut self, code: impl Into<String>, message: impl Into<String>) {
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

/// A status change that contradicts the generations already recorded
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct IllegalStatusTransitionError {
    message: String,
}

impl IllegalStatusTransitionError {
    fn new(message: String) -> Self {
        Self { message }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::{ResourceHeadersExt, ResourceID};

    const GENERATION: u64 = 2;

    #[derive(Clone, Copy, Debug)]
    enum Deletion {
        None,
        Requested,
        Completed,
    }

    // (observed generation, reconciled generation, has a reconciliation error,
    // phase without deletion)
    const CASES: &[(Option<u64>, Option<u64>, bool, ResourcePhase)] = &[
        (None, None, false, ResourcePhase::Pending),
        (None, None, true, ResourcePhase::Pending),
        (Some(1), None, false, ResourcePhase::Pending),
        (Some(1), None, true, ResourcePhase::Pending),
        (Some(1), Some(1), false, ResourcePhase::Pending),
        (Some(1), Some(1), true, ResourcePhase::Pending),
        (Some(2), None, false, ResourcePhase::Reconciling),
        (Some(2), None, true, ResourcePhase::Failed),
        (Some(2), Some(1), false, ResourcePhase::Reconciling),
        (Some(2), Some(1), true, ResourcePhase::Degraded),
        (Some(2), Some(2), false, ResourcePhase::Ready),
        (Some(2), Some(2), true, ResourcePhase::Ready),
    ];

    #[test]
    fn test_derive_phase_over_all_inputs() {
        for &(observed, reconciled, has_error, phase) in CASES {
            for deletion in [Deletion::None, Deletion::Requested, Deletion::Completed] {
                let expected = match deletion {
                    Deletion::None => phase,
                    Deletion::Requested => ResourcePhase::Deleting,
                    Deletion::Completed => ResourcePhase::Deleted,
                };

                let status = status(observed, reconciled, has_error);
                assert_eq!(
                    status.derive_phase(&headers(deletion)),
                    expected,
                    "observed {observed:?}, reconciled {reconciled:?}, error {has_error}, \
                     deletion {deletion:?}"
                );
            }
        }
    }

    fn status(observed: Option<u64>, reconciled: Option<u64>, has_error: bool) -> ResourceStatus {
        let mut status = new_pending_resource_status();
        status.observed_generation = observed;
        status.reconciled_generation = reconciled;
        if has_error {
            status.mark_failed_or_degraded("code", "message");
        }
        status
    }

    fn headers(deletion: Deletion) -> ResourceHeaders {
        let mut headers = ResourceHeaders::simple(
            now(),
            ResourceID::new(uuid::Uuid::nil()),
            odf::ResourceOwnerHandle::new_test("owner"),
            "res",
        );
        headers.generation = GENERATION;
        match deletion {
            Deletion::None => {}
            Deletion::Requested => headers.deletion_requested_at = Some(now()),
            Deletion::Completed => {
                headers.deletion_requested_at = Some(now());
                headers.deleted_at = Some(now());
            }
        }
        headers
    }

    fn now() -> DateTime<Utc> {
        DateTime::UNIX_EPOCH
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
