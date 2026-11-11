// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use database_common::DatabaseTransactionRunner;
use internal_error::InternalError;

use crate::controller_runtime::{
    ResourceControllerCandidate,
    ResourceControllerPass,
    ResourceControllerRegistration,
};
use crate::domain::{ResourceControllerStateRepository, ResourceID};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Selects the resources one drain pass processes.
///
/// Two sources feed a pass, with different suppression:
///
/// | Source | Selected when | Left out while |
/// | --- | --- | --- |
/// | Eligibility predicates (storage) | not reconciled, deletion pending, or status changed for a controller reacting to status | finished at the current version and revision, or waiting for a retry or failure backoff |
/// | `list_extra_actionable` (controller) | the controller says so, e.g. an expired claim | waiting for a retry or failure backoff, or finished with earlier in this drain |
///
/// An extra resource ignores `Done` on purpose: being listed is the signal
/// that it needs attention although nothing about the resource changed. A
/// backoff still applies, so a failing extra resource is not retried at once.
pub(crate) struct ResourceControllerPassPlanner<'a> {
    registration: &'a ResourceControllerRegistration,
    catalog: &'a dill::Catalog,
    batch_size: usize,
}

impl<'a> ResourceControllerPassPlanner<'a> {
    pub fn new(
        registration: &'a ResourceControllerRegistration,
        catalog: &'a dill::Catalog,
        batch_size: usize,
    ) -> Self {
        Self {
            registration,
            catalog,
            batch_size,
        }
    }

    /// Plans a pass at `now`. `finished_extras` are the extra resources the
    /// controller returned `Done` for earlier in this drain: listing them
    /// again breaks the controller's contract, and processing them again would
    /// never let the drain end.
    pub async fn plan(
        &self,
        now: DateTime<Utc>,
        finished_extras: &HashSet<ResourceID>,
    ) -> Result<ResourceControllerPass, InternalError> {
        // The controller opens its own transactions, so it is asked before ours
        let extras = self
            .registration
            .controller()
            .list_extra_actionable(now, self.batch_size)
            .await?;

        // Both sources are read in one transaction, so that the bookkeeping of
        // the extra resources and the eligibility query see the same state
        DatabaseTransactionRunner::new(self.catalog.clone())
            .transactional_with(
                |repository: Arc<dyn ResourceControllerStateRepository>| async move {
                    let mut pass = ResourceControllerPass::default();
                    // A resource can be both extra and eligible; it is processed once
                    let mut planned = HashSet::new();

                    for id in extras {
                        if planned.insert(id) {
                            self.plan_extra(
                                repository.as_ref(),
                                id,
                                now,
                                finished_extras,
                                &mut pass,
                            )
                            .await?;
                        }
                    }

                    let eligible = repository
                        .find_actionable_resource_ids(
                            self.registration.actionable_query(),
                            now,
                            self.batch_size,
                        )
                        .await?;
                    for id in eligible {
                        if planned.insert(id) {
                            pass.add_candidate(ResourceControllerCandidate::eligible(id));
                        }
                    }

                    Ok(pass)
                },
            )
            .await
    }

    /// Adds an extra resource to the pass, or defers it while it backs off
    async fn plan_extra(
        &self,
        repository: &dyn ResourceControllerStateRepository,
        id: ResourceID,
        now: DateTime<Utc>,
        finished_extras: &HashSet<ResourceID>,
        pass: &mut ResourceControllerPass,
    ) -> Result<(), InternalError> {
        let controller_name = &self.registration.meta().controller_name;

        if finished_extras.contains(&id) {
            tracing::warn!(
                controller = controller_name.as_str(),
                resource_id = %id,
                "Extra resource still listed after its controller finished with it in this drain"
            );
            return Ok(());
        }

        // A row with `not_before` records a retry or a failure. Without it the
        // controller finished with the resource, which an extra one ignores.
        let not_before = repository
            .get_state(controller_name, &id)
            .await?
            .and_then(|state| state.not_before);

        match not_before {
            Some(not_before) if not_before > now => pass.defer_extra(not_before),
            Some(_) | None => pass.add_candidate(ResourceControllerCandidate::extra(id)),
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
