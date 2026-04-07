// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourcesSummary {
    pub resource_counts: Vec<ResourceTypeCountSummary>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceTypeCountSummary {
    pub kind: String,
    pub name: String,
    pub api_version: String,
    pub total_count: u64,
    pub phase_counts: ResourcePhaseCounts,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceSummaryRow {
    pub kind: String,
    pub api_version: String,
    pub total_count: u64,
    pub phase_counts: ResourcePhaseCounts,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourcePhaseCounts {
    pub pending: u64,
    pub reconciling: u64,
    pub ready: u64,
    pub degraded: u64,
    pub failed: u64,
}

impl ResourcePhaseCounts {
    pub fn increment_pending(&mut self) {
        self.pending += 1;
    }

    pub fn increment_reconciling(&mut self) {
        self.reconciling += 1;
    }

    pub fn increment_ready(&mut self) {
        self.ready += 1;
    }

    pub fn increment_degraded(&mut self) {
        self.degraded += 1;
    }

    pub fn increment_failed(&mut self) {
        self.failed += 1;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
