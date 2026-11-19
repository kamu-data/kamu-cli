// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};

use crate::controller_runtime::ResourceControllerCandidate;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// One pass of a drain: the resources to process now, and the earliest moment
/// an extra resource left out for its backoff becomes due.
///
/// The second part exists because the bookkeeping storage cannot see extra
/// resources: they come from the controller. When the controller reports an
/// extra resource that is already due but still backing off, the loop must
/// sleep until the backoff ends, not wake at the long-passed due time again
/// and again.
#[derive(Debug, Default)]
pub(crate) struct ResourceControllerPass {
    candidates: Vec<ResourceControllerCandidate>,
    earliest_deferred_extra: Option<DateTime<Utc>>,
    /// Whether the drain's hold-back left out a resource this pass would
    /// otherwise have processed
    held_back: bool,
}

impl ResourceControllerPass {
    pub fn add_candidate(&mut self, candidate: ResourceControllerCandidate) {
        self.candidates.push(candidate);
    }

    /// Records an extra resource left out until `not_before`
    pub fn defer_extra(&mut self, not_before: DateTime<Utc>) {
        self.earliest_deferred_extra = Some(
            self.earliest_deferred_extra
                .map_or(not_before, |earliest| earliest.min(not_before)),
        );
    }

    /// Records a resource left out because the drain holds it back
    pub fn note_held_back(&mut self) {
        self.held_back = true;
    }

    pub fn held_back(&self) -> bool {
        self.held_back
    }

    pub fn is_empty(&self) -> bool {
        self.candidates.is_empty()
    }

    pub fn earliest_deferred_extra(&self) -> Option<DateTime<Utc>> {
        self.earliest_deferred_extra
    }

    pub fn into_candidates(self) -> Vec<ResourceControllerCandidate> {
        self.candidates
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
