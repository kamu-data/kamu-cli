// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::Deserialize;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AdminSummaryQueryDataFragment {
    pub admin: AdminResourcesFieldFragment,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AdminResourcesFieldFragment {
    pub resources: AdminSummaryFieldFragment,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AdminSummaryFieldFragment {
    pub summary: ResourcesSummaryFragment,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SummaryQueryDataFragment {
    pub resources: SummaryFragment,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SummaryFragment {
    pub summary: ResourcesSummaryFragment,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ResourcesSummaryFragment {
    pub resource_counts: Vec<ResourceTypeCountSummaryFragment>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ResourceTypeCountSummaryFragment {
    pub kind: String,
    pub name: String,
    pub api_version: String,
    pub total_count: u64,
    pub phase_counts: ResourcePhaseCountsFragment,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ResourcePhaseCountsFragment {
    pub pending: u64,
    pub reconciling: u64,
    pub ready: u64,
    pub degraded: u64,
    pub failed: u64,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
