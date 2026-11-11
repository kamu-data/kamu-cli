// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};

use crate::controller_runtime::{
    ResourceControllerCandidate,
    ResourceControllerCandidateOrigin,
    ResourceProcessAttempt,
};
use crate::domain::{ResourceID, ResourceProcessOutcome};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// How many times one drain may ask about one resource. A multi-step
/// reconciliation whose steps are all due at once uses a few; more means the
/// controller never settles.
pub(crate) const MAX_PROCESS_CALLS_PER_DRAIN: usize = 10;

/// The resources one drain does not ask about again, whatever the next pass
/// selects. The bookkeeping suppresses most resources the drain processed,
/// but two cases escape it and would keep the drain going forever:
///
/// - an extra resource the controller finished with: `Done` does not suppress an extra resource,
///   and the controller may keep listing it. It is held back only as an extra resource: once it
///   changes, the eligibility predicates select it again, and it is processed;
/// - a resource the controller never settles: one that keeps returning a `RetryAt` already due, or
///   `Done` with a version older than its own writes. It is held back once it reaches
///   `MAX_PROCESS_CALLS_PER_DRAIN`.
///
/// Holding back lasts as long as the caller keeps the value: the agent's loop
/// starts each drain with a new one, so a misbehaving controller is slowed
/// down, never starved of its resources; a drain session keeps one across its
/// drains, so that they end.
#[derive(Debug, Default)]
pub(crate) struct ResourceControllerDrainHoldBack {
    finished_extras: HashSet<ResourceID>,
    process_calls: HashMap<ResourceID, usize>,
    over_limit: HashSet<ResourceID>,
}

impl ResourceControllerDrainHoldBack {
    /// Takes note of one finished attempt
    pub fn note_attempt(
        &mut self,
        candidate: &ResourceControllerCandidate,
        attempt: &ResourceProcessAttempt,
        controller_name: &str,
    ) {
        let id = candidate.id();

        if candidate.origin() == ResourceControllerCandidateOrigin::Extra
            && matches!(
                attempt,
                ResourceProcessAttempt::Processed(ResourceProcessOutcome::Done(_))
            )
        {
            self.finished_extras.insert(id);
        }

        let calls = self.process_calls.entry(id).or_default();
        *calls += 1;
        if *calls >= MAX_PROCESS_CALLS_PER_DRAIN && self.over_limit.insert(id) {
            tracing::warn!(
                controller = controller_name,
                resource_id = %id,
                calls = *calls,
                "Controller did not settle the resource in this drain, holding it back until the next one"
            );
        }
    }

    /// Whether the resource, listed as an extra one, is not asked about
    pub fn holds_back_extra(&self, id: &ResourceID) -> bool {
        self.finished_extras.contains(id) || self.over_limit.contains(id)
    }

    /// Whether the resource, selected by the eligibility predicates, is not
    /// asked about. A finished extra resource is not held back here: the
    /// predicates suppress one finished at its current version, so selecting
    /// it means it changed since.
    pub fn holds_back_eligible(&self, id: &ResourceID) -> bool {
        self.over_limit.contains(id)
    }

    /// An extra resource the controller finished with and still lists, which
    /// breaks its contract
    pub fn is_finished_extra(&self, id: &ResourceID) -> bool {
        self.finished_extras.contains(id)
    }

    /// How many resources are held back. The eligibility query asks for this
    /// many more rows, so that held back resources cannot fill a whole batch
    /// and hide the ones still waiting.
    pub fn len(&self) -> usize {
        self.finished_extras.union(&self.over_limit).count()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
