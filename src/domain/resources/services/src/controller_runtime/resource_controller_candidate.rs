// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::ResourceID;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A resource a drain pass asks its controller about
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ResourceControllerCandidate {
    id: ResourceID,
    origin: ResourceControllerCandidateOrigin,
}

/// Why a resource became a candidate. It decides which suppression applies:
/// see `ResourceControllerPassPlanner`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResourceControllerCandidateOrigin {
    /// Selected by the generic eligibility predicates of the bookkeeping
    /// storage, which already applied every suppression
    Eligible,
    /// Listed by the controller's `list_extra_actionable`. Its `Done` does not
    /// suppress it, but a pending retry or failure backoff does.
    Extra,
}

impl ResourceControllerCandidate {
    pub fn eligible(id: ResourceID) -> Self {
        Self {
            id,
            origin: ResourceControllerCandidateOrigin::Eligible,
        }
    }

    pub fn extra(id: ResourceID) -> Self {
        Self {
            id,
            origin: ResourceControllerCandidateOrigin::Extra,
        }
    }

    pub fn id(&self) -> ResourceID {
        self.id
    }

    pub fn origin(&self) -> ResourceControllerCandidateOrigin {
        self.origin
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
