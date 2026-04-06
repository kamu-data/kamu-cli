// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{ApplyResourceOutcome, ApplyResourceRejectionCategory, ResourceView};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ApplyManifestPlan {
    pub resource: ResourceView,
    pub outcome: ApplyResourceOutcome,
    pub reconciliation_required: bool,
    pub executable: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ApplyManifestResult {
    pub resource: ResourceView,
    pub outcome: ApplyResourceOutcome,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ApplyManifestRejection {
    pub category: ApplyResourceRejectionCategory,
    pub message: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub enum ApplyManifestPlanningDecision {
    Planned(ApplyManifestPlan),
    Rejected(ApplyManifestRejection),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub enum ApplyManifestApplicationDecision {
    Applied(ApplyManifestResult),
    Rejected(ApplyManifestRejection),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
